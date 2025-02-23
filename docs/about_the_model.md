# Sobre o Modelo / About the Model

📌 Introdução / Introduction
----------------------------

O **DeputyRecommender** é um sistema de recomendação desenvolvido para identificar deputados com perfis semelhantes com base em diversas características, incluindo ideologia, classificação partidária, participação em proposições legislativas e utilização de recursos. Ele utiliza aprendizado de máquina para processar os dados e calcular similaridades entre parlamentares.

The **DeputyRecommender** is a recommendation system designed to identify deputies with similar profiles based on various characteristics, including ideology, party classification, participation in legislative proposals, and resource usage. It employs machine learning to process data and compute similarities between representatives.

* * *

🔍 Estrutura do Modelo / Model Structure
----------------------------------------

O modelo segue os seguintes passos principais:

1.  **Carregamento e limpeza dos dados**
    
    *   Substituição de valores infinitos por `NaN`
    *   Preenchimento de valores ausentes:
        *   Mediana para colunas numéricas
        *   Valor mais frequente para colunas categóricas
2.  **Definição das características**
    
    *   As features são categorizadas em três níveis de importância: **Alta**, **Média** e **Baixa**
    *   Atribuição de pesos para influenciar a similaridade
3.  **Pré-processamento dos dados**
    
    *   Pipeline para normalização de variáveis numéricas
    *   Codificação One-Hot para variáveis categóricas
    *   Aplicação de pesos conforme a importância das features
4.  **Cálculo da similaridade**
    
    *   Utilização do **cosine similarity** para medir a proximidade entre deputados
5.  **Recomendações**
    
    *   Identificação dos deputados mais similares com base na matriz de similaridade
    *   Retorno de informações-chave sobre as semelhanças entre os políticos recomendados

The model follows these main steps:

1.  **Data loading and cleaning**
    
    *   Replacing infinite values with `NaN`
    *   Filling missing values:
        *   Median for numerical columns
        *   Most frequent value for categorical columns
2.  **Feature definition**
    
    *   Features are categorized into three levels of importance: **High**, **Medium**, and **Low**
    *   Assigning weights to influence similarity
3.  **Data preprocessing**
    
    *   Pipeline for numerical variable normalization
    *   One-Hot Encoding for categorical variables
    *   Applying weights according to feature importance
4.  **Similarity calculation**
    
    *   Using **cosine similarity** to measure proximity between deputies
5.  **Recommendations**
    
    *   Identifying the most similar deputies based on the similarity matrix
    *   Returning key insights on similarities among recommended politicians

* * *

🛠️ Tecnologias e Bibliotecas / Technologies and Libraries
----------------------------------------------------------

O sistema foi desenvolvido utilizando as seguintes bibliotecas:

*   `pandas` e `numpy` para manipulação de dados
*   `scikit-learn` para pré-processamento, normalização e cálculo de similaridade
*   `scipy.sparse` para lidar com matrizes esparsas
*   `joblib` para persistência do modelo

The system was developed using the following libraries:

*   `pandas` and `numpy` for data manipulation
*   `scikit-learn` for preprocessing, normalization, and similarity calculation
*   `scipy.sparse` for handling sparse matrices
*   `joblib` for model persistence

* * *

🎯 Objetivo / Goal
------------------

O objetivo do **DeputyRecommender** é fornecer recomendações precisas e interpretáveis para identificar deputados similares, auxiliando análises políticas e comparações entre legisladores.

The goal of **DeputyRecommender** is to provide accurate and interpretable recommendations to identify similar deputies, supporting political analysis and comparisons among legislators.

* * *

Se precisar de ajustes ou quiser incluir mais detalhes, me avise!

