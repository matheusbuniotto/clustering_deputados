import duckdb
import os
import json
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(dotenv_path=env_path)

# Get the BASE_DIR from environment variables
base_dir = os.getenv('BASE_DIR')

# Check if BASE_DIR was loaded correctly
if base_dir is None:
    raise ValueError("BASE_DIR environment variable not set. Please check your .env file.")

# Ensure the script operates in the correct directory
os.chdir(base_dir)
# Load the ingestion configuration from JSON file
with open(os.path.join(base_dir, 'data', 'bronze', 'ingestion.json'), 'r') as f:
    config = json.load(f)

# Define the path to the raw data CSV files
csv_file_path = os.path.join(base_dir, 'data', 'raw', config['csv_file_name'])

# Connect to DuckDB and run the query on the CSV files
con = duckdb.connect(database=':memory:')
query = config['query'].format(csv_file_path=csv_file_path)
result = con.execute(query).fetchdf()

# Define the output CSV file path
output_csv_path = os.path.join(base_dir, 'data', 'bronze', config['output_table_name'])

# Save the result to a CSV file
result.to_csv(output_csv_path, index=False)

# Print the result
print(f"Query result saved to {output_csv_path}")