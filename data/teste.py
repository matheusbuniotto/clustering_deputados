import os
import boto3
import duckdb
import io
from botocore.exceptions import NoCredentialsError

# Ensure the script operates in the correct directory
base_dir = '/home/matheus/Projetos/gh/clustering_deputados'
os.chdir(base_dir)

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define the bucket name, S3 folder, and prefix
bucket_name = 'proj-deputados-fiap'
s3_folder = 'raw'
prefix = 'deputies'

# Define the DuckDB file path
duckdb_file_path = os.path.join(base_dir, 'data', 'bronze', 'cleaned_data.duckdb')

try:
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder)
    
    for obj in response.get('Contents', []):
        s3_key = obj['Key']
        file_name = os.path.basename(s3_key)
        
        if file_name.startswith(prefix):
            # Download the file content to memory
            csv_buffer = io.BytesIO()
            s3_client.download_fileobj(bucket_name, s3_key, csv_buffer)
            csv_buffer.seek(0)
            
            # Process the data with DuckDB
            con = duckdb.connect(database=duckdb_file_path)
            con.execute(f"CREATE TABLE IF NOT EXISTS raw_data AS SELECT * FROM read_csv_auto('{csv_buffer.getvalue().decode()}', delim=';', header=True, encoding='latin1')")
            cleaned_df = con.execute("SELECT * FROM raw_data").df()
            
            # Save the cleaned data to DuckDB
            con.execute("INSERT INTO raw_data SELECT * FROM cleaned_df")
            print(f"Data from {s3_key} processed and stored in {duckdb_file_path}")

except NoCredentialsError:
    print("Credentials not available.")
except Exception as e:
    print(f"An error occurred: {e}")