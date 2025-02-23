import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.metrics.pairwise import cosine_similarity
import scipy.sparse
import joblib
import os

class DeputyRecommender:
    def __init__(self, data_path, model_path='model'):
        self.data_path = data_path
        self.model_path = model_path
        self.df = pd.read_csv(data_path)
        self._clean_data()
        self._prepare_features()
        self.preprocessor = self._create_preprocessor()
        
        if self._model_exists():
            self._load_model()
        else:
            self.processed_data = self._preprocess_data()
            self.similarity_matrix = self._compute_similarity()
            self._save_model()

    def _clean_data(self):
        # Adjust some columns with inf numbers 
        self.df.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # Separate numeric and non-numeric columns
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        non_numeric_cols = self.df.select_dtypes(exclude=[np.number]).columns
        
        # Replace Nulls with the mmedian value in non numeric columns
        self.df[numeric_cols] = self.df[numeric_cols].fillna(self.df[numeric_cols].median())
        
        # Replace Nulls with the most comun value in non numeric columns
        self.df[non_numeric_cols] = self.df[non_numeric_cols].fillna(self.df[non_numeric_cols].mode().iloc[0])

    def _prepare_features(self):
        self.feature_info = {
            'high': {
                'numerical': ['populist_elements', 'proposition_count', 'cost_per_proposition'],
                'categorical': ['ideology', 'party_classification', 'agenda_category']
            },
            'medium': {
                'numerical': ['attendance_rate', 'total_documents', 'unjustified_absence_count',
                             'share_taxi_toll_parking', 'share_flight_passages', 
                             'share_office_maintenance', 'share_fuel_lubricants'],
                'categorical': []
            },
            'low': {
                'numerical': [],
                'categorical': ['state', 'party']
            }
        }

    def _create_preprocessor(self):
        numerical_features = (
            self.feature_info['high']['numerical'] +
            self.feature_info['medium']['numerical'] +
            self.feature_info['low']['numerical']
        )
        
        categorical_features = (
            self.feature_info['high']['categorical'] +
            self.feature_info['medium']['categorical'] +
            self.feature_info['low']['categorical']
        )

        numerical_pipeline = Pipeline([
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
        ])

        categorical_pipeline = Pipeline([
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('encoder', OneHotEncoder(handle_unknown='ignore', sparse_output=True))
        ])

        return ColumnTransformer([
            ('num', numerical_pipeline, numerical_features),
            ('cat', categorical_pipeline, categorical_features)
        ])

    def _calculate_feature_weights(self, feature_names):
        weights = []
        for name in feature_names:
            parts = name.split('__')
            feature_type = parts[0]
            original_feature = parts[1].split('_')[0] if feature_type == 'cat' else parts[1]
            
            importance = 'low'
            for imp_level in ['high', 'medium', 'low']:
                if original_feature in self.feature_info[imp_level]['numerical'] + \
                   self.feature_info[imp_level]['categorical']:
                    importance = imp_level
                    break
            
            weight = 3 if importance == 'high' else 2 if importance == 'medium' else 1
            weights.append(weight)
        
        return np.array(weights)

    def _preprocess_data(self):
        processed = self.preprocessor.fit_transform(self.df)
        feature_names = self.preprocessor.get_feature_names_out()
        weights = self._calculate_feature_weights(feature_names)
        
        if scipy.sparse.issparse(processed):
            processed = processed.multiply(weights).tocsr()
        else:
            processed = processed * weights
        
        return processed

    def _compute_similarity(self):
        return cosine_similarity(self.processed_data)

    def _model_exists(self):
        return os.path.exists(f'{self.model_path}/data.pkl') and os.path.exists(f'{self.model_path}/similarity.pkl')

    def _save_model(self):
        os.makedirs(self.model_path, exist_ok=True)
        joblib.dump(self.processed_data, f'{self.model_path}/data.pkl')
        joblib.dump(self.similarity_matrix, f'{self.model_path}/similarity.pkl')

    def _load_model(self):
        self.processed_data = joblib.load(f'{self.model_path}/data.pkl')
        self.similarity_matrix = joblib.load(f'{self.model_path}/similarity.pkl')

    def recommend(self, deputy_name, top_n=5):
        if deputy_name not in self.df['name'].values:
            raise ValueError(f"Deputado '{deputy_name}' não encontrado")

        deputy_idx = self.df.index[self.df['name'] == deputy_name][0]
        similarities = list(enumerate(self.similarity_matrix[deputy_idx]))
        sorted_similarities = sorted(similarities, key=lambda x: x[1], reverse=True)
        
        results = []
        seen_names = set()
        
        for idx, score in sorted_similarities:
            if len(results) >= top_n:
                break
            current_deputy = self.df.iloc[idx]
            if current_deputy['name'] != deputy_name and current_deputy['name'] not in seen_names:
                seen_names.add(current_deputy['name'])
                results.append({
                    'name': current_deputy['name'],
                    'similarity_score': round(score, 4),
                    'key_similarities': self._get_key_similarities(self.df.iloc[deputy_idx], current_deputy),
                    'most_similar_fields': self._get_most_similar_fields(self.df.iloc[deputy_idx], current_deputy)
                })
        
        return {
            'input_deputy': deputy_name,
            'similar_deputies': results[:top_n]
        }

    def recommend_by_id(self, deputy_id, top_n=5):
        if deputy_id not in self.df['deputy_id'].values:
            raise ValueError(f"Deputado com ID '{deputy_id}' não encontrado")

        deputy_idx = self.df.index[self.df['deputy_id'] == deputy_id][0]
        similarities = list(enumerate(self.similarity_matrix[deputy_idx]))
        sorted_similarities = sorted(similarities, key=lambda x: x[1], reverse=True)
        
        results = []
        seen_ids = set()
        
        for idx, score in sorted_similarities:
            if len(results) >= top_n:
                break
            current_deputy = self.df.iloc[idx]
            if current_deputy['deputy_id'] != deputy_id and current_deputy['deputy_id'] not in seen_ids:
                seen_ids.add(current_deputy['deputy_id'])
                results.append({
                    'deputy_id': current_deputy['deputy_id'],
                    'name': current_deputy['name'],
                    'similarity_score': round(score, 4),
                    'key_similarities': self._get_key_similarities(self.df.iloc[deputy_idx], current_deputy),
                    'most_similar_fields': self._get_most_similar_fields(self.df.iloc[deputy_idx], current_deputy)
                })
        
        return {
            'input_deputy_id': deputy_id,
            'similar_deputies': results[:top_n]
        }

    def recommend_by_name(self, deputy_name, top_n=5):
        if deputy_name not in self.df['name'].values:
            raise ValueError(f"Deputado '{deputy_name}' não encontrado")

        deputy_id = self.df.loc[self.df['name'] == deputy_name, 'deputy_id'].values[0]
        return self.recommend_by_id(deputy_id, top_n)

    def _get_key_similarities(self, source, target):
        similarities = {}
        important_features = [
            ('ideology', 'Categoria Ideológica'),
            ('party_classification', 'Classificação Partidária'),
            ('agenda_category', 'Categoria da Agenda'),
            ('proposition_count', 'Proposições Legislativas'),
            ('cost_per_proposition', 'Custo por Proposição'),
            ('attendance_rate', 'Frequência em Sessões')
        ]
        
        for field, label in important_features:
            if source[field] == target[field]:
                similarities[label] = f"Igual: {source[field]}"
            elif isinstance(source[field], (int, float)):
                diff = abs(source[field] - target[field])
                similarities[label] = f"Diferença: {round(diff, 2)}"
            else:
                similarities[label] = f"Original: {source[field]} → Similar: {target[field]}"
        
        return similarities

    def _get_most_similar_fields(self, source, target):
        most_similar_fields = []
        important_features = [
            'ideology', 'party_classification', 'agenda_category',
            'proposition_count', 'cost_per_proposition', 'attendance_rate'
        ]
        
        for field in important_features:
            if source[field] == target[field]:
                most_similar_fields.append(field)
        
        return most_similar_fields

if __name__ == "__main__":
    recommender = DeputyRecommender('enriched_df.csv')

