
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

INSERT INTO dw.dim_customers (CustomerID, CustomerName)
SELECT DISTINCT 
    cs."CustomerKey", 
    COALESCE(c."FirstName" || ' ' || c."LastName", 'Unknown Customer')
FROM public.cleaned_sales cs
LEFT JOIN public.customers c ON cs."CustomerKey" = c."CustomerKey";

INSERT INTO dw.dim_products (ProductID, ProductName, CategoryID, SubcategoryID)
SELECT DISTINCT 
    p."ProductKey", 
    COALESCE(NULLIF(p."ProductName", ''), 'Unknown Product') AS ProductName, 
    COALESCE(ps."ProductCategoryKey", 0) AS CategoryID, 
    COALESCE(p."ProductSubcategoryKey", 0) AS SubcategoryID
FROM public.products p
LEFT JOIN public.product_subcategories ps 
    ON p."ProductSubcategoryKey" = ps."ProductSubcategoryKey"
ON CONFLICT (ProductID) DO UPDATE
   SET ProductName = EXCLUDED.ProductName,
       CategoryID = EXCLUDED.CategoryID,
       SubcategoryID = EXCLUDED.SubcategoryID;

INSERT INTO dw.conflict_log (TableName, ConflictDetails)
SELECT 'dim_products', 'Conflict occurred on ProductID: ' || p."ProductKey"
FROM public.products p
WHERE EXISTS (
    SELECT 1
    FROM dw.dim_products dp
    WHERE dp.ProductID = p."ProductKey"
);

INSERT INTO dw.dim_territories (TerritoryID, TerritoryName)
SELECT DISTINCT 
    t."SalesTerritoryKey" AS TerritoryID, 
    t."Region" || ', ' || t."Country" AS TerritoryName
FROM public.territories t;

INSERT INTO dw.dim_calendar (DateID, Year, Month, Day, Weekday)
SELECT DISTINCT 
    DATE("OrderDate"), 
    EXTRACT(YEAR FROM "OrderDate"), 
    EXTRACT(MONTH FROM "OrderDate"), 
    EXTRACT(DAY FROM "OrderDate"), 
    TO_CHAR("OrderDate", 'Day')
FROM public.cleaned_sales;

INSERT INTO dw.fact_sales (SalesID, CustomerID, ProductID, TerritoryID, DateID, Quantity, UnitPrice, TotalSales)
SELECT 
    cs."OrderNumber" || '-' || cs."ProductKey" AS SalesID,
    cs."CustomerKey",
    cs."ProductKey",
    cs."TerritoryKey",
    DATE(cs."OrderDate"),
    cs."OrderQuantity",
    p."ProductPrice",
    cs."OrderQuantity" * p."ProductPrice"
FROM public.cleaned_sales cs
LEFT JOIN public.products p ON cs."ProductKey" = p."ProductKey"
WHERE p."ProductPrice" IS NOT NULL
ON CONFLICT (SalesID) DO UPDATE
SET Quantity = EXCLUDED.Quantity,
    UnitPrice = EXCLUDED.UnitPrice,
    TotalSales = EXCLUDED.TotalSales;

-- Update missing product names
UPDATE dw.dim_products dp
SET ProductName = p."ProductName"
FROM public.products p
WHERE dp.ProductID = p."ProductKey"
  AND (dp.ProductName IS NULL OR dp.ProductName = '');

-- Update missing territory names
UPDATE dw.dim_territories dt
SET TerritoryName = t."Region" || ', ' || t."Country"
FROM public.territories t
WHERE dt.TerritoryID = t."SalesTerritoryKey"
  AND (dt.TerritoryName IS NULL OR dt.TerritoryName = '');

