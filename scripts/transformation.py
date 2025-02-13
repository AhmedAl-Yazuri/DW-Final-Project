import pandas as pd
from sqlalchemy import create_engine
import json
from IPython.display import display  

# Load database configuration
with open('config/db_config.json') as config_file:
    config = json.load(config_file)

DB_URI = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
engine = create_engine(DB_URI)

# Function to load, clean, and transform products data
def transform_products():
    print("Loading and transforming products data...")
    
    products_df = pd.read_csv('Data/raw/Products.csv', skiprows=1)
    
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
        
        # Download the cleaned data as CSV
        products_df.to_csv('Data/processed/Products_Cleaned.csv', index=False)
        print("Cleaned products data downloaded as CSV.")
    else:
        print("Error: Missing essential columns in products data.")

# Function to load, clean, and transform sales data from multiple years
def transform_sales():
    print("Loading and transforming sales data...")
    
    # Load sales data from multiple years
    sales_2015 = pd.read_csv('Data/raw/Sales_2015.csv')
    sales_2016 = pd.read_csv('Data/raw/Sales_2016.csv')
    sales_2017 = pd.read_csv('Data/raw/Sales_2017.csv')
    
    # Combine all sales data 
    combined_sales = pd.concat([sales_2015, sales_2016, sales_2017])
    
    # Remove rows with missing essential fields
    combined_sales.dropna(subset=['OrderDate', 'ProductKey', 'CustomerKey', 'OrderQuantity'], inplace=True)
    
    # Ensure OrderDate is in datetime format
    combined_sales['OrderDate'] = pd.to_datetime(combined_sales['OrderDate'], errors='coerce')
    
    # Save the cleaned DataFrame to the PostgreSQL database
    combined_sales.to_sql('cleaned_sales', engine, if_exists='replace', index=False)
    print("Sales data transformed and saved successfully.")
    
    # Download the cleaned data as CSV
    combined_sales.to_csv('Data/processed/Sales_Cleaned.csv', index=False)
    print("Cleaned sales data downloaded as CSV.")

# Function to load, clean, and transform customers data
def transform_customers():
    print("Loading and transforming customers data...")

    try:
        customers_df = pd.read_csv('Data/raw/Customers.csv', encoding='ISO-8859-1')
    except UnicodeDecodeError:
        print("Error: Failed to read the file with 'ISO-8859-1' encoding. Trying 'latin1' encoding...")
        customers_df = pd.read_csv('Data/raw/Customers.csv', encoding='latin1')
    
    # Fill missing Gender values with the most common gender
    most_common_gender = customers_df['Gender'].mode()[0]
    customers_df['Gender'].fillna(most_common_gender, inplace=True)
    
    # Fill missing Prefix values based on Gender
    customers_df['Prefix'] = customers_df.apply(
        lambda row: 'Mr.' if pd.isnull(row['Prefix']) and row['Gender'] == 'Male' else 
                    ('Ms.' if pd.isnull(row['Prefix']) and row['Gender'] == 'Female' else 
                     ('undefined' if pd.isnull(row['Prefix']) else row['Prefix'])), axis=1
    )

    # Save the cleaned DataFrame to the PostgreSQL database
    customers_df.to_sql('customers', engine, if_exists='replace', index=False)
    print("Customers data transformed and saved successfully.")

    # Download the cleaned data as CSV
    customers_df.to_csv('Data/processed/Customers_Cleaned.csv', index=False)
    print("Cleaned customers data downloaded as CSV.")

transform_products()
transform_sales()
transform_customers()
