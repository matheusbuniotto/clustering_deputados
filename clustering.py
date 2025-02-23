import pandas as pd
from utils.data_processing import read_parquet_s3, clean_data

# imports
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import AgglomerativeClustering
from scipy.cluster.hierarchy import dendrogram, linkage
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import seaborn as sns


#df = read_parquet_s3()

df = pd.read_parquet('data/gold/deputies_consolidated_metrics_parquet')


df_vars = df[['proposition_count', 'total_expenses']].dropna()
scaler = StandardScaler()
df_vars_norm = scaler.fit_transform(df_vars)

df_vars_norm = pd.DataFrame(df_vars_norm, columns = ['proposition_count', 'total_expenses'])

range_n_clusters = [2, 3, 4, 5, 6, 7, 8, 9, 10]

for n_clusters in range_n_clusters:
    np.random.seed(42)
    
    clustering = AgglomerativeClustering(n_clusters = n_clusters, linkage = 'ward')
    cluster_labels = clustering.fit_predict(df_vars_norm)
    
    silhouette_avg = silhouette_score(df_vars_norm, cluster_labels)
    
    print(f"For n_clusters = {n_clusters}, the average silhouette_score is : {silhouette_avg}")


for n_clusters in range(2, 8):
    np.random.seed(42)
    modelo = AgglomerativeClustering(n_clusters = n_clusters, linkage='ward')
    clusters = modelo.fit_predict(df_vars_norm)
   
    # Criando um DataFrame
    df_vars_norm.loc[:,'cluster'] = clusters
    
    plt.figure(figsize=(6, 4))
    plt.scatter(df_vars_norm['total_expenses'], df_vars_norm['proposition_count'], c=df_vars_norm['cluster'], cmap='viridis', alpha=0.7)
    plt.xlabel('Gastos', fontsize = 12)
    plt.ylabel('Proposições', fontsize = 12)
    plt.title(f'Nº de Clusters: {n_clusters}', fontsize = 12)
    plt.legend()
    
    # plt.savefig(f'clustering_{n_clusters}_clusters.png')
    
    plt.show()