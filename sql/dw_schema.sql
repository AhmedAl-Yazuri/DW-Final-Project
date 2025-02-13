
-- sql/dw_schema.sql
-- This script creates the data warehouse schema with detailed schema definitions.

DROP SCHEMA IF EXISTS dw CASCADE;
CREATE SCHEMA dw;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_name = 'product_subcategories_pkey'
          AND table_name = 'product_subcategories'
    ) THEN
        ALTER TABLE public.product_subcategories
        ADD CONSTRAINT product_subcategories_pkey PRIMARY KEY ("ProductSubcategoryKey");
    END IF;
END $$;

CREATE TABLE dw.dim_customers (
    CustomerID BIGINT PRIMARY KEY,
    CustomerName TEXT
);

CREATE TABLE dw.dim_products (
    ProductID BIGINT PRIMARY KEY,
    ProductName TEXT,
    CategoryID BIGINT,
    SubcategoryID BIGINT
);

CREATE TABLE dw.dim_territories (
    TerritoryID BIGINT PRIMARY KEY,
    TerritoryName TEXT
);

CREATE TABLE dw.dim_calendar (
    DateID DATE PRIMARY KEY,
    Year INT,
    Month INT,
    Day INT,
    Weekday TEXT
);

CREATE TABLE dw.fact_sales (
    SalesID TEXT PRIMARY KEY,
    CustomerID BIGINT REFERENCES dw.dim_customers(CustomerID),
    ProductID BIGINT REFERENCES dw.dim_products(ProductID),
    TerritoryID BIGINT REFERENCES dw.dim_territories(TerritoryID),
    DateID DATE REFERENCES dw.dim_calendar(DateID),
    Quantity INT,
    UnitPrice NUMERIC,
    TotalSales NUMERIC
);

CREATE TABLE IF NOT EXISTS dw.conflict_log (
    ConflictTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    TableName TEXT,
    ConflictDetails TEXT
);

