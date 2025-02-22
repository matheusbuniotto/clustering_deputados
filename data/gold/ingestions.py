import os
import duckdb

# Create a DuckDB connection to the bronze_db
bronze_con = duckdb.connect('../bronze/bronze_db.db')

# Create a DuckDB connection to the silver_db
silver_con = duckdb.connect('silver_db.db')

# Iterate over the .sql files in the current folder
for sql_file in os.listdir('.'):
    if sql_file.endswith('.sql'):
        table_name = sql_file.replace('.sql', '')
        
        # Read the SQL query from the file
        with open(sql_file, 'r') as open_file:
            query = open_file.read()
        
        # Execute the query on the bronze_db
        result_df = bronze_con.execute(query).fetchdf()
        
        # Save the result to the silver_db
        silver_con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM result_df;")
        print(f"File {sql_file} processed and saved the to table {table_name} in silver_db")

# Close the DuckDB connections
bronze_con.close()
silver_con.close()
