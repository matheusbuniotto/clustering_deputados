import os
import boto3
import argparse

def upload_to_s3(bucket_name, s3_base_folder, ignore_folders=None):
    if ignore_folders is None:
        ignore_folders = []

    s3_client = boto3.client('s3')
    local_base_folder = os.path.dirname(os.path.abspath(__file__))

    def upload_directory(local_folder, s3_folder):
        for root, dirs, files in os.walk(local_folder):
            for file in files:
                if file.endswith('.csv') or 'parquet' in file:
                    file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(file_path, local_base_folder)
                    s3_key = os.path.join(s3_folder, relative_path).replace("\\", "/")
                    s3_client.upload_file(file_path, bucket_name, s3_key)
                    print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")

    for folder in ['silver', 'raw', 'bronze', 'gold']:
        if folder not in ignore_folders:
            local_folder = os.path.join(local_base_folder, folder)
            if os.path.exists(local_folder):
                upload_directory(local_folder, s3_base_folder)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Upload files to S3.')
    parser.add_argument('--ignore', nargs='*', default=[], help='Folders to ignore')
    args = parser.parse_args()

    bucket_name = "proj-deputados-fiap"
    s3_base_folder = ""
    ignore_folders = args.ignore

    upload_to_s3(bucket_name, s3_base_folder, ignore_folders)