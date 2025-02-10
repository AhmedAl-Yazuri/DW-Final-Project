import os
import pandas as pd
from sqlalchemy import create_engine
import json

with open('config/db_config.json') as config_file:
    config = json.load(config_file)

DB_URI = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}?options=-csearch_path=dw"
engine = create_engine(DB_URI)

def load_queries(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    queries = content.split(';')  
    return [query.strip() for query in queries if query.strip()]  

def fetch_stats():
    queries = load_queries('sql\queries.sql')

    total_sales = pd.read_sql_query(queries[0], engine)
    unique_customers = pd.read_sql_query(queries[1], engine)
    top_products = pd.read_sql_query(queries[2], engine)
    top_regions = pd.read_sql_query(queries[3], engine)
    sales_by_year = pd.read_sql_query(queries[4], engine)
    sales_by_weekday = pd.read_sql_query(queries[5], engine)

    os.makedirs('reports', exist_ok=True)

    with open('reports/stats_report.md', 'w') as report:
        report.write("# Data Warehouse Statistics\n\n")

        report.write(f"**Total Sales:** {total_sales.iloc[0, 0]:,.2f}\n\n")
        report.write(f"**Unique Customers:** {unique_customers.iloc[0, 0]}\n\n")

        report.write("**Top 5 Products by Sales:**\n")
        report.write(top_products.to_markdown(index=False))
        report.write("\n\n")

        report.write("**Top 5 Regions by Revenue:**\n")
        report.write(top_regions.to_markdown(index=False))
        report.write("\n\n")

        report.write("**Total Sales by Year:**\n")
        report.write(sales_by_year.to_markdown(index=False))
        report.write("\n\n")

        report.write("**Sales by Weekday:**\n")
        report.write(sales_by_weekday.to_markdown(index=False))
        report.write("\n\n")

    print("Statistics extracted and saved to reports/stats_report.md")

fetch_stats()
