import pandas as pd
from sqlalchemy import create_engine
import json
from IPython.display import display  # For better display of DataFrames in Jupyter notebooks

with open('config/db_config.json') as config_file:
    config = json.load(config_file)

DB_URI = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
engine = create_engine(DB_URI)


# Function to load, clean, and transform products data
def transform_products():
    print("Loading and transforming products data...")
    
    # Load product data from CSV, skipping the first row if it's metadata
    products_df = pd.read_csv('Data/raw/Products.csv', skiprows=1)
    
    # Remove unnamed columns (likely artifacts from CSV exports)
    products_df = products_df.loc[:, ~products_df.columns.str.contains('^Unnamed')]
    
    # Remove columns labeled 'Delete me' (if such columns exist)
    products_df = products_df.loc[:, ~products_df.columns.str.contains('Delete me', case=False)]
    
    # Check if required columns exist before proceeding
    required_columns = ['ProductKey', 'ProductSubcategoryKey', 'ProductName']
    if all(col in products_df.columns for col in required_columns):
        products_df.dropna(subset=['ProductKey', 'ProductSubcategoryKey'], inplace=True)
        
   
        
        # Save the cleaned DataFrame to the PostgreSQL database in a table named 'products'
        products_df.to_sql('products', engine, if_exists='replace', index=False)
        print("Products data transformed and saved successfully.")
    else:
        print("Error: Missing essential columns in products data.")

# Function to load, clean, and transform sales data from multiple years
def transform_sales():
    print("Loading and transforming sales data...")
    
    sales_2015 = pd.read_csv('Data/raw/Sales_2015.csv')
    sales_2016 = pd.read_csv('Data/raw/Sales_2016.csv')
    sales_2017 = pd.read_csv('Data/raw/Sales_2017.csv')
    
    # Combine all sales data 
    combined_sales = pd.concat([sales_2015, sales_2016, sales_2017])
    
    # Remove rows with missing essential fields
    combined_sales.dropna(subset=['OrderDate', 'ProductKey', 'CustomerKey', 'OrderQuantity'], inplace=True)
    
    combined_sales['OrderDate'] = pd.to_datetime(combined_sales['OrderDate'], errors='coerce')
    
  
    
    # Save the cleaned DataFrame to the PostgreSQL database 
    combined_sales.to_sql('cleaned_sales', engine, if_exists='replace', index=False)
    print("Sales data transformed and saved successfully.")

# Call the functions to transform products and sales data
transform_products()
transform_sales()
