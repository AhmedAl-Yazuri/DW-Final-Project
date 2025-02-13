from sqlalchemy import create_engine
import pandas as pd
import json
import os

# Load DB configuration
with open('config\db_config.json') as config_file:
    config = json.load(config_file)

DB_URI = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"

engine = create_engine(DB_URI)

# Directory path for raw data
data_dir = '' 

files = {
    'sales_2015': 'Data/raw/Sales_2015.csv',
    'sales_2016': 'Data/raw/Sales_2016.csv',
    'sales_2017': 'Data/raw/Sales_2017.csv',
    'territories': 'Data/raw/Territories.csv',
    'calendar': 'Data/raw/Calendar.csv',
    'customers': 'Data/raw/Customers.csv',
    'product_categories': 'Data/raw/Product_Categories.csv',
    'product_subcategories': 'Data/raw/Product_Subcategories.csv',
    'products': 'Data/raw/Products.csv',
    'returns': 'Data/raw/Returns.csv'
}
# Ingest data into PostgreSQL
def ingest_file(file_name, table_name):
    file_path = os.path.join(data_dir, file_name)
    print(f"Ingesting {file_name} into table {table_name}...")
    
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
    except UnicodeDecodeError:
        print(f"UnicodeDecodeError: Trying with ISO-8859-1 encoding for {file_name}")

        df = pd.read_csv(file_path, encoding='ISO-8859-1')
    
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"{table_name} ingestion complete.")

# Loop through files and ingest them
for table_name, file_name in files.items():
    ingest_file(file_name, table_name)

print("All files ingested successfully.")
