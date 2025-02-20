import os
import json
import boto3
import duckdb
import io
from botocore.exceptions import NoCredentialsError

def upload_files_to_s3(folder_path, bucket_name, s3_folder=""):
    """
    Uploads all CSV files from a local folder to an S3 bucket.
    Carrega todos os arquivos CSV de uma pasta local para um bucket S3.
        folder_path (str): Path to the local folder containing the files.
                           Caminho para a pasta local contendo os arquivos.
        bucket_name (str): Name of the S3 bucket.
                           Nome do bucket S3.
        s3_folder (str, optional): S3 folder path where files will be uploaded. Defaults to "".
                                   Caminho da pasta S3 onde os arquivos serão carregados. Padrão é "".
    """
    s3_client = boto3.client('s3')
    
    # Iterate over the files in the folder
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

def download_files_from_s3(bucket_name, s3_folder, local_folder, prefix):
    """
    Download files from a specified S3 folder to a local folder.
    Baixa arquivos de uma pasta S3 especificada para uma pasta local.

    Args:
        bucket_name (str): the bucket name in s3
        s3_folder (str): folder name in s3
        local_folder (str): local folder containing the csv files
        prefix (str): prefix of the file name
    """
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

def process_and_upload_in_memory(bucket_name, s3_folder, prefix, query):
    """
    Process data in memory and upload to S3.
    Processa dados na memória e carrega para o S3.

    Args:
        bucket_name (str): the bucket name in s3
        s3_folder (str): folder name in s3
        prefix (str): prefix of the file name
        query (str): SQL query for data processing
    """
    s3_client = boto3.client('s3')
    
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
                con = duckdb.connect(database=':memory:')
                con.execute(f"CREATE TABLE raw_data AS SELECT * FROM read_csv_auto('{csv_buffer.getvalue().decode()}', delim=';', header=True, encoding='latin1')")
                cleaned_df = con.execute(query).df()
                
                # Save the cleaned data to memory
                cleaned_csv_buffer = io.StringIO()
                cleaned_df.to_csv(cleaned_csv_buffer, index=False, sep=";")
                cleaned_csv_buffer.seek(0)
                
                # Upload the cleaned data to S3
                cleaned_s3_key = os.path.join(s3_folder, f"cleaned_{file_name}")
                s3_client.put_object(Bucket=bucket_name, Key=cleaned_s3_key, Body=cleaned_csv_buffer.getvalue())
                print(f"Uploaded cleaned data to s3://{bucket_name}/{cleaned_s3_key}")
    
    except NoCredentialsError:
        print("Credentials not available.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Load configuration from JSON file
    with open("ingestion_config.json", "r") as config_file:
        config = json.load(config_file)
    
    # Get the current working directory
    current_dir = os.getcwd()
    
    # Update the local folder paths with the current directory
    upload_config = config["upload"]
    upload_config["local_folder"] = os.path.join(current_dir, upload_config["local_folder"])
    
    download_config = config["download"]
    download_config["local_folder"] = os.path.join(current_dir, download_config["local_folder"])
    
    # Read the query from the SQL file
    query_file_path = os.path.join(current_dir, config["query_file"])
    with open(query_file_path, "r") as query_file:
        query = query_file.read()
    
    # Upload files to S3
    upload_files_to_s3(upload_config["local_folder"], upload_config["bucket_name"], upload_config["s3_folder"])
    
    # Download files from S3
    download_files_from_s3(download_config["bucket_name"], download_config["s3_folder"], download_config["local_folder"], download_config["prefix"])
    
    # Process and upload data in memory
    process_and_upload_in_memory(download_config["bucket_name"], download_config["s3_folder"], download_config["prefix"], query)