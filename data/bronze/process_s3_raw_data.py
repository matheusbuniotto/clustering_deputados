import os
import boto3
import duckdb
from botocore.exceptions import NoCredentialsError

def download_files_from_s3(bucket_name, s3_folder, local_folder, prefix):
    s3_client = boto3.client('s3')
    
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder)
        
        for obj in response.get('Contents', []):
            s3_key = obj['Key']
            file_name = os.path.basename(s3_key)
            
            if file_name.startswith(prefix):
                local_file_path = os.path.join(local_folder, file_name)
                s3_client.download_file(bucket_name, s3_key, local_file_path)
                print(f"Downloaded {s3_key} to {local_file_path}")
    
    except NoCredentialsError:
        print("Credentials not available.")
    except Exception as e:
        print(f"An error occurred: {e}")

def clean_data_with_duckdb(file_path, query):
    con = duckdb.connect(database=':memory:')
    con.execute(f"CREATE TABLE raw_data AS SELECT * FROM read_csv_auto('{file_path}', delim=';', header=True, encoding='latin1')")
    
    # Execute the cleaning query
    cleaned_df = con.execute(query).df()
    return cleaned_df

def upload_files_to_s3(folder_path, bucket_name, s3_folder=""):
    s3_client = boto3.client('s3')
    
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                s3_key = os.path.join(s3_folder, file)
                
                try:
                    s3_client.upload_file(file_path, bucket_name, s3_key)
                    print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
                except FileNotFoundError:
                    print(f"The file {file_path} was not found.")
                except NoCredentialsError:
                    print("Credentials not available.")
                except Exception as e:
                    print(f"An error occurred: {e}")
                    

raw_bucket_name = "proj-deputados-fiap"
raw_s3_folder = "raw/"
local_raw_folder = "bronze"
bronze_bucket_name = "proj-deputados-fiap"
bronze_s3_folder = "bronze/"
local_bronze_folder = "data/bronze"
prefix = "deputies"  # Replace with the actual prefix
 
# Download files from S3
download_files_from_s3(raw_bucket_name, raw_s3_folder, local_raw_folder, prefix)
    

if __name__ == "__main__":
    raw_bucket_name = "proj-deputados-fiap"
    raw_s3_folder = "raw/"
    local_raw_folder = "data/raw"
    bronze_bucket_name = "proj-deputados-fiap"
    bronze_s3_folder = "bronze/"
    local_bronze_folder = "data/bronze"
    prefix = "your_prefix"  # Replace with the actual prefix
    
    # Download files from S3
    download_files_from_s3(raw_bucket_name, raw_s3_folder, local_raw_folder, prefix)
    
    # Define your cleaning query
    query = """
    -- Your SQL query here
    SELECT * FROM raw_data
    """
    
    # Clean and save the data
    for root, dirs, files in os.walk(local_raw_folder):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                cleaned_df = clean_data_with_duckdb(file_path, query)
                cleaned_file_path = os.path.join(local_bronze_folder, file)
                cleaned_df.to_csv(cleaned_file_path, index=False, sep=";")
    
    # Upload cleaned files to S3
    upload_files_to_s3(local_bronze_folder, bronze_bucket_name, bronze_s3_folder)