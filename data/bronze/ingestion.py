import os
import json
import duckdb

# Load the JSON configuration
with open('sources.json', 'r') as open_file:
    sources = json.load(open_file)

# Create a DuckDB connection in the current folder
con = duckdb.connect('bronze_db.db')

# Track successfully created tables
created_tables = []

# Iterate over the CSV files specified in the JSON
for csv_file in sources['csv_files']:
    start_name = csv_file['start_name']
    table_name = csv_file['table_name']
    
    # Find the CSV file in the ../raw directory
    for file in os.listdir('../raw'):
        if file.startswith(start_name) and file.endswith('.csv'):
            csv_path = os.path.join('../raw', file)
            
            # Read the CSV file to get the column names and types
            df = duckdb.read_csv(csv_path, sample_size=1)
            columns = ', '.join([f"{col} {dtype}" for col, dtype in zip(df.columns, df.dtypes)])
            
            # Create the table if it does not exist
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});")
            
            # Ingest the CSV file into the DuckDB table
            con.execute(f"COPY {table_name} FROM '{csv_path}' (AUTO_DETECT TRUE, strict_mode false);")
            print(f"Ingested {csv_path} into table {table_name}")
            
            # Add to created tables list
            created_tables.append(table_name)

# Export the tables to CSV files in the current folder
for table_name in created_tables:
    export_path = f"{table_name}.csv"
    con.execute(f"COPY {table_name} TO '{export_path}' (HEADER, DELIMITER ',');")
    print(f"Exported table {table_name} to {export_path}")

# Close the DuckDB connection
con.close()