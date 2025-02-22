import pandas as pd

# Define the S3 bucket and file path
bucket_name = 'proj-deputados-fiap'
file_path = 'silver/deputies_attendance_parquet'

# Construct the full S3 path
s3_path = f's3://{bucket_name}/{file_path}'

# Read the Parquet file into a DataFrame
df = pd.read_parquet(s3_path, storage_options={'anon': False})

# Display the DataFrame
print(df.head())
# %%
