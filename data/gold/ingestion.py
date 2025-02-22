import os
import duckdb

# Create a DuckDB connection to the deputies_db
con = duckdb.connect('../deputies_db.db')

# Create the gold schema if it doesn't exist
con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

# Iterate over the .sql files in the current folder
for sql_file in os.listdir('.'):
    if sql_file.endswith('.sql'):
        table_name = sql_file.replace('.sql', '')
        
        # Read the SQL query from the file
        with open(sql_file, 'r') as open_file:
            query = open_file.read()
        
        # Execute the query on the deputies_db
        result_df = con.execute(query).fetchdf()
        
        # Save the result to the gold schema
        con.execute(f"CREATE OR REPLACE TABLE gold.{table_name} AS SELECT * FROM result_df;")
        print(f"File {sql_file} processed and saved the result to table gold.{table_name}")
        
        # Export the result to a Parquet file partitioned by party
        parquet_path = f"{table_name}_parquet"
        con.execute(f"COPY (SELECT * FROM gold.{table_name}) TO '{parquet_path}' (FORMAT PARQUET, PARTITION_BY party);")
        print(f"Exported table gold.{table_name} to Parquet files partitioned by party at {parquet_path}")

# Close the DuckDB connection
con.close()