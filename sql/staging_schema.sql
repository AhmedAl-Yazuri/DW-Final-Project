-- Drop staging schema if it exists
DROP SCHEMA IF EXISTS staging CASCADE;
CREATE SCHEMA staging;

-- Create staging table for Customers
CREATE TABLE staging.staging_customers (
    CustomerKey BIGINT,
    FirstName TEXT,
    LastName TEXT
);

-- Create staging table for Products
CREATE TABLE staging.staging_products (
    ProductKey BIGINT,
    ProductName TEXT,
    ProductCategoryKey BIGINT,
    ProductSubcategoryKey BIGINT,
    ProductPrice NUMERIC
);

-- Create staging table for Territories
CREATE TABLE staging.staging_territories (
    SalesTerritoryKey BIGINT,
    Region TEXT,
    Country TEXT
);

-- Create staging table for Sales
CREATE TABLE staging.staging_sales (
    OrderNumber TEXT,
    CustomerKey BIGINT,
    ProductKey BIGINT,
    TerritoryKey BIGINT,
    OrderDate DATE,
    OrderQuantity INT
);
