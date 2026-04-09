USE BillingDW

CREATE TABLE dim_customer (
    customer_id   VARCHAR(20)   NOT NULL,
    customer_name VARCHAR(100)  NOT NULL,
    country       VARCHAR(60)   NULL,
    region        VARCHAR(60)   NULL,
    industry      VARCHAR(60)   NULL,
    segment       VARCHAR(60)   NULL,
    CONSTRAINT PK_dim_customer PRIMARY KEY (customer_id)
);

CREATE TABLE dim_service (
    service_id    VARCHAR(20)   NOT NULL,
    service_name  VARCHAR(100)  NOT NULL,
    category      VARCHAR(60)   NULL,
    sub_category  VARCHAR(60)   NULL,
    unit_type     VARCHAR(40)   NULL,
    CONSTRAINT PK_dim_service PRIMARY KEY (service_id)
);


CREATE TABLE dim_date (
    date_id      DATE          NOT NULL,
    full_date    DATE          NOT NULL,
    year         INT           NOT NULL,
    quarter      INT           NOT NULL,
    month        INT           NOT NULL,
    month_name   VARCHAR(12)   NOT NULL,
    week         INT           NOT NULL,
    day_of_week  VARCHAR(12)   NOT NULL,
    is_weekend   BIT           NOT NULL,
    CONSTRAINT PK_dim_date PRIMARY KEY (date_id)
);


-- Populate dim_date for 2020-2026
WITH dates AS (
    SELECT CAST(CAST('2020-01-01' AS DATE) AS DATE) AS d
    UNION ALL
    SELECT DATEADD(day, 1, d) FROM dates
    WHERE d < '2026-12-31'
)
INSERT INTO dim_date
SELECT
    d,
    d,
    YEAR(d),
    DATEPART(quarter, d),
    MONTH(d),
    DATENAME(month, d),
    DATEPART(week, d),
    DATENAME(weekday, d),
    CASE WHEN DATEPART(weekday, d) IN (1, 7) THEN 1 ELSE 0 END
FROM dates
OPTION (MAXRECURSION 3000);


CREATE TABLE fact_billing (
    billing_id   VARCHAR(20)      NOT NULL,
    customer_id  VARCHAR(20)      NOT NULL,
    service_id   VARCHAR(20)      NOT NULL,
    date_id      DATE             NOT NULL,
    amount       DECIMAL(12, 2)   NOT NULL,
    units_used   INT              NOT NULL,
    unit_price   DECIMAL(10, 4)   NOT NULL,
    CONSTRAINT PK_fact_billing
        PRIMARY KEY (billing_id),
    CONSTRAINT FK_fact_customer
        FOREIGN KEY (customer_id)
        REFERENCES dim_customer(customer_id),
    CONSTRAINT FK_fact_service
        FOREIGN KEY (service_id)
        REFERENCES dim_service(service_id),
    CONSTRAINT FK_fact_date
        FOREIGN KEY (date_id)
        REFERENCES dim_date(date_id)
);


-- 1. Create the staging table
CREATE TABLE stg_billing (
    billing_id    VARCHAR(20),
    customer_id   VARCHAR(20),
    customer_name VARCHAR(100),
    country       VARCHAR(60),
    industry      VARCHAR(60),
    category      VARCHAR(60),
    service_name  VARCHAR(100),
    billing_date  VARCHAR(20),
    amount        VARCHAR(20),
    units_used    VARCHAR(20),
    unit_price    VARCHAR(20)
);

DROP TABLE IF EXISTS stg_billing;


-- ── dim_customer (skip existing) ─────────────────────────────────────────
INSERT INTO dim_customer (customer_id, customer_name, country, industry)
SELECT DISTINCT
    s.customer_id,
    s.customer_name,
    s.country,
    s.industry
FROM stg_billing s
WHERE s.customer_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM dim_customer c
      WHERE c.customer_id = s.customer_id
  );

-- ── dim_service (stable ID = checksum of name, skip existing) ────────────
INSERT INTO dim_service (service_id, service_name, category)
SELECT DISTINCT
    CONCAT('SVC-', ABS(CHECKSUM(s.service_name))) AS service_id,
    s.service_name,
    s.category
FROM stg_billing s
WHERE s.service_name IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM dim_service sv
      WHERE sv.service_name = s.service_name
  );



-- Verify row counts
SELECT 'dim_customer' AS tbl, COUNT(*) AS rows FROM dim_customer
UNION ALL
SELECT 'dim_service',  COUNT(*) FROM dim_service
UNION ALL
SELECT 'dim_date',     COUNT(*) FROM dim_date;



-- ── fact_billing (upsert via MERGE) ──────────────────────────────────────
MERGE fact_billing AS target
USING (
    SELECT
        s.billing_id,
        c.customer_id,
        sv.service_id,
        CAST(s.billing_date AS DATE)        AS date_id,
        CAST(s.amount       AS DECIMAL(12,2)) AS amount,
        CAST(s.units_used   AS INT)           AS units_used,
        CAST(s.unit_price   AS DECIMAL(10,4)) AS unit_price
    FROM stg_billing s
    INNER JOIN dim_customer c  ON s.customer_id  = c.customer_id
    INNER JOIN dim_service  sv ON s.service_name = sv.service_name
    WHERE s.billing_id IS NOT NULL
) AS source
ON target.billing_id = source.billing_id

WHEN MATCHED THEN
    UPDATE SET
        customer_id = source.customer_id,
        service_id  = source.service_id,
        date_id     = source.date_id,
        amount      = source.amount,
        units_used  = source.units_used,
        unit_price  = source.unit_price

WHEN NOT MATCHED THEN
    INSERT (billing_id, customer_id, service_id,
            date_id, amount, units_used, unit_price)
    VALUES (source.billing_id, source.customer_id, source.service_id,
            source.date_id, source.amount, source.units_used, source.unit_price);


SELECT 'dim_customer' AS tbl, COUNT(*) AS rows FROM dim_customer
UNION ALL SELECT 'dim_service',  COUNT(*) FROM dim_service
UNION ALL SELECT 'dim_date',     COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_billing', COUNT(*) FROM fact_billing;


--Average billing per customer

SELECT
    c.customer_name,
    AVG(f.amount) AS avg_billing
FROM       fact_billing f
INNER JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY   c.customer_name
ORDER BY   avg_billing DESC;

--Billing by country

SELECT
    c.country,
    SUM(f.amount) AS total_billing
FROM       fact_billing f
INNER JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY   c.country
ORDER BY   total_billing DESC;

--Billing by industry and service category

SELECT
    c.industry,
    s.category,
    SUM(f.amount) AS total_billing
FROM       fact_billing f
INNER JOIN dim_customer c  ON f.customer_id = c.customer_id
INNER JOIN dim_service  s  ON f.service_id  = s.service_id
GROUP BY   c.industry, s.category
ORDER BY   c.industry, total_billing DESC;

--Monthly billing trend

SELECT
    d.year,
    d.month_name,
    SUM(f.amount)  AS total_billing,
    COUNT(*)       AS num_invoices
FROM       fact_billing f
INNER JOIN dim_date     d ON f.date_id = d.date_id
GROUP BY   d.year, d.month, d.month_name
ORDER BY   d.year, d.month;
