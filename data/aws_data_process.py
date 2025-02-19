import os
import boto3
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
        bucket_name (_type_): the bucket name in s3
        s3_folder (_type_): folder name in 3s
        local_folder (_type_): local folder containing the csv files
        prefix (_type_): prefix of the file name
    """
    # Initialize the S3 client
    s3_client = boto3.client('s3')
    
    # Create the local folder if it doesn't exist
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)
    
    try:
        # List objects in the specified S3 folder
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_folder)
        
        for obj in response.get('Contents', []):
            s3_key = obj['Key']
            file_name = os.path.basename(s3_key)
            
            # Check if the file name starts with the specified prefix
            if file_name.startswith(prefix):
                local_file_path = os.path.join(local_folder, file_name)
                
                # Download the file from S3
                s3_client.download_file(bucket_name, s3_key, local_file_path)
                print(f"Downloaded {s3_key} to {local_file_path}")
    
    except NoCredentialsError:
        print("Credentials not available.")
    except Exception as e:
        print(f"An error occurred: {e}")



if __name__ == "__main__":
    # Specify the folders
    folder_path = "data/raw" 
    bucket_name = "proj-deputados-fiap"
    s3_folder = "raw" 
    
    # Execute 
    upload_files_to_s3(folder_path, bucket_name, s3_folder)
    
    