import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import matplotlib.pyplot as plt

def load_and_preprocess(filepath):
    """Enhanced data loading with validation"""
    # Add Parquet read configuration
    df = pd.read_parquet(
        filepath,
        engine='pyarrow',
        use_threads=True
    )
    
    # Validate required columns
    required_cols = ['name', 'total_expenses', 'attendance_rate', 
                    'proposition_count', 'party_classification', 'party']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Enhanced numeric cleaning with logging
    num_cols = ['total_expenses', 'attendance_rate', 'proposition_count']
    for col in num_cols:
        initial_nulls = df[col].isna().sum()
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(r'[^\d.]', '', regex=True), 
            errors='coerce'
        )
        new_nulls = df[col].isna().sum() - initial_nulls
        if new_nulls > 0:
            print(f"Cleaned {new_nulls} invalid values in {col}")
    
    # Improved missing value handling
    imputation_values = {
        'total_expenses': df['total_expenses'].median(),
        'attendance_rate': df['attendance_rate'].median(),
        'proposition_count': 0
    }
    df.fillna(imputation_values, inplace=True)
    
    # Calculate cost per proposition
    df['cost_per_proposition'] = df['total_expenses'] / df['proposition_count']
    df['cost_per_proposition'].replace([np.inf, -np.inf], np.nan, inplace=True)
    df['cost_per_proposition'].fillna(df['cost_per_proposition'].median(), inplace=True)
    
    return df

def create_recommender_model(df):
    """Create recommendation pipeline"""
    # Define features
    features = ['total_expenses', 'attendance_rate', 
                'proposition_count', 'cost_per_proposition', 'party_classification', 'party']
    
    # Preprocessing pipeline
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), ['total_expenses', 'attendance_rate', 'proposition_count', 'cost_per_proposition']),
            ('cat', OneHotEncoder(handle_unknown='ignore'), ['party_classification', 'party'])
        ])
    
    # KNN model
    knn = NearestNeighbors(n_neighbors=5, metric='cosine')
    
    # Full pipeline
    pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('knn', knn)
    ])
    
    pipeline.fit(df[features])
    return pipeline

def get_similar_deputies(name, df, model):
    """Get recommendations for a specific deputy"""
    if name not in df['name'].values:
        return f"No deputy found with name: {name}"
    
    deputy_index = df[df['name'] == name].index[0]
    features = ['total_expenses', 'attendance_rate', 
                'proposition_count', 'cost_per_proposition', 'party_classification', 'party']
    
    distances, indices = model.named_steps['knn'].kneighbors(
        model.named_steps['preprocessor'].transform(
            df[features].iloc[[deputy_index]]
        )
    )
    
    recommendations = df.iloc[indices[0]].copy()
    recommendations['similarity_score'] = 1 - distances[0]
    return recommendations[['name', 'party_classification', 'similarity_score',
                            'total_expenses', 'attendance_rate',
                            'proposition_count', 'cost_per_proposition', 'party']]

def visualize_similarity(target, recommendations):
    """Visual comparison of features"""
    features = ['total_expenses', 'attendance_rate', 'proposition_count', 'cost_per_proposition']
    
    plt.figure(figsize=(10, 6))
    radar = plt.subplot(111, polar=True)
    
    # Normalize features for radar chart
    normalized = pd.concat([target[features], recommendations[features]]).apply(
        lambda x: (x - x.min()) / (x.max() - x.min())
    )
    
    angles = np.linspace(0, 2 * np.pi, len(features), endpoint=False).tolist()
    angles += angles[:1]  # Close the polygon
    
    # Plot target
    values = normalized.iloc[0].values.tolist() + [normalized.iloc[0].values[0]]
    radar.plot(angles, values, linewidth=2, label=target['name'].values[0])
    
    # Plot recommendations
    for i in range(1, len(normalized)):
        values = normalized.iloc[i].values.tolist() + [normalized.iloc[i].values[0]]
        radar.plot(angles, values, linestyle='--', 
                 label=recommendations['name'].values[i-1])
    
    radar.set_theta_offset(np.pi/2)
    radar.set_theta_direction(-1)
    radar.set_thetagrids(np.degrees(angles[:-1]), features)
    plt.title("Legislative Profile Comparison", pad=20)
    plt.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1))
    plt.show()

def main():
    df = load_and_preprocess('data/gold/deputies_consolidated_metrics_parquet')
    model = create_recommender_model(df)
    
    while True:
        name = input("\nEnter deputy name (or 'quit' to exit): ").strip()
        if name.lower() == 'quit':
            break
            
        recommendations = get_similar_deputies(name, df, model)
        
        if isinstance(recommendations, str):
            print(recommendations)
            continue
            
        print("\nMost similar deputies:")
        print(recommendations.to_string(index=False))
        
        target = df[df['name'] == name]
        visualize_similarity(target, recommendations.iloc[1:])

if __name__ == "__main__":
    main()