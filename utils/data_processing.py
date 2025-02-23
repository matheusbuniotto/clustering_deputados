def read_parquet_s3(bucket_name='proj-deputados-fiap', file_path='gold/deputies_consolidated_metrics_parquet'):
    import pandas as pd

    bucket_name = bucket_name
    file_path = file_path

    # S3 path completo
    s3_path = f's3://{bucket_name}/{file_path}'
    df = pd.read_parquet(s3_path, storage_options={'anon': False})

    return df


def clean_data(df):
    import pandas as pd
    df = df.copy()
    
    # Clean numerical columns
    num_cols = ['total_expenses', 'attendance_rate', 'proposition_count']
    for col in num_cols:
        df[col] = pd.to_numeric(df[col].str.replace('[^\d.]', '', regex=True), errors='coerce')
    
    # Handle missing values
    df['attendance_rate'] = df['attendance_rate'].fillna(df['attendance_rate'].mean())
    df['proposition_count'] = df['proposition_count'].fillna(0)
    
    return df