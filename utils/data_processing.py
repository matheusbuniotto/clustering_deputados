import pandas as pd
import json
import os

from gpt_classifier import classify_party


def read_parquet_s3(bucket_name='proj-deputados-fiap', file_path='gold/deputies_consolidated_metrics_parquet'):
    bucket_name = bucket_name
    file_path = file_path

    # S3 path completo
    s3_path = f's3://{bucket_name}/{file_path}'
    df = pd.read_parquet(s3_path, storage_options={'anon': False})

    return df


def enrich_data_with_gpt_classification(main_df, classification_df) -> pd.DataFrame:
    
    # Create a DataFrame from the JSON data
    normalized_classification = pd.json_normalize(classification_df['classification'])
    
    classification_df = classification_df.drop(columns=['classification'])
    
    classification_df = pd.concat([classification_df, normalized_classification], axis=1)

    final_df = main_df.merge(classification_df, on='deputy_id', how='left')
    return final_df


def feature_engineer(enriched_df) -> pd.DataFrame:
    enriched_df = enriched_df.copy()
        
    max_days = enriched_df.total_days.max()
    
    # JSON Path
    party_ideology_map_path = '../data/party_ideology_map.json'
    
    # Check existance of file
    if os.path.exists(party_ideology_map_path):
  
        with open(party_ideology_map_path, 'r') as f:
            party_ideology_map = json.load(f)
    else:
        # Run GPT function if the map doestn exist
        parties_to_classify = enriched_df[enriched_df['party_classification'].isnull()]['party'].unique()
        party_ideology_map = {party: classify_party(party) for party in parties_to_classify}
        
        # Save dict as JSON
        with open(party_ideology_map_path, 'w') as f:
            json.dump(party_ideology_map, f, ensure_ascii=False, indent=4)
    
    enriched_df = enriched_df.assign(
        cost_per_proposition=(enriched_df['total_expenses'] / enriched_df['proposition_count']).round(1),
        mean_expend_per_document=(enriched_df['total_expenses'] / enriched_df['total_documents']).round(1),
        share_taxi_toll_parking=(enriched_df['taxi_toll_parking'] / enriched_df['total_expenses']).round(3),
        share_flight_passages=(enriched_df['flight_passages'] / enriched_df['total_expenses']).round(3),
        share_office_maintenance=(enriched_df['office_maintenance'] / enriched_df['total_expenses']).round(3),
        share_fuel_lubricants=(enriched_df['fuel_lubricants'] / enriched_df['total_expenses']).round(3),
        attendance_rate = (enriched_df['attendance_count'] / max_days).round(3),
        party_classification = enriched_df["party_classification"].fillna(
            enriched_df["party"].map(party_ideology_map))  
    )
    
    return enriched_df 


def main():
    df = read_parquet_s3(file_path='gold/deputies_consolidated_metrics_parquet')
    gpt_classifications = read_parquet_s3(file_path='gold/classified_deputies.parquet')

    enriched_df = enrich_data_with_gpt_classification(df, gpt_classifications).drop(columns=['propositions_list'])

    df_ = feature_engineer(enriched_df)

    df_.to_csv('../data/enriched_df.csv', index=False)
    
if __name__ == "__main__":
    main()