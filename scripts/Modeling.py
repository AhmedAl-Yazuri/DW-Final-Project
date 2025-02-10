from sqlalchemy import create_engine, text
import json

# Load DB configuration
with open('config/db_config.json') as config_file:
    config = json.load(config_file)

DB_URI = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
engine = create_engine(DB_URI)

try:
    # Read the SQL schema from file
    with open('sql\dw_schema.sql', 'r') as file:
        schema_sql = file.read()

    # Execute the schema creation
    with engine.connect() as conn:
        conn.execute(text(schema_sql))
        print("Data warehouse schema dropped, recreated, and populated successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
