-- Total Sales
SELECT SUM(totalsales) AS Total_Sales FROM dw.fact_sales;

-- Unique Customers
SELECT COUNT(DISTINCT customerid) AS Unique_Customers FROM dw.dim_customers;

-- Top 5 Products by Sales
SELECT dp.productname, COUNT(fs.salesid) AS Sales_Count
FROM dw.fact_sales fs
JOIN dim_products dp ON fs.productid = dp.productid
GROUP BY dp.productname
ORDER BY Sales_Count DESC
LIMIT 5;

-- Top 5 Regions by Revenue
SELECT dt.territoryname, SUM(fs.totalsales) AS Total_Revenue
FROM dw.fact_sales fs
JOIN dim_territories dt ON fs.territoryid = dt.territoryid
GROUP BY dt.territoryname
ORDER BY Total_Revenue DESC
LIMIT 5;

-- Total Sales by Year
SELECT dc.Year, SUM(fs.TotalSales) AS Total_Sales
FROM dw.fact_sales fs
JOIN dim_calendar dc ON fs.DateID = dc.DateID
GROUP BY dc.Year
ORDER BY dc.Year;

-- Sales by Weekday
SELECT dc.Weekday, SUM(fs.TotalSales) AS Total_Sales
FROM dw.fact_sales fs
JOIN dim_calendar dc ON fs.DateID = dc.DateID
GROUP BY dc.Weekday
ORDER BY 
    CASE dc.Weekday
        WHEN 'Sunday' THEN 1
        WHEN 'Monday' THEN 2
        WHEN 'Tuesday' THEN 3
        WHEN 'Wednesday' THEN 4
        WHEN 'Thursday' THEN 5
        WHEN 'Friday' THEN 6
        WHEN 'Saturday' THEN 7
    END;
