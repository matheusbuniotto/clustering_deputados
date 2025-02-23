🇬🇧 **About the Model**

🔎 **Intelligent Recommendations for Political Analysis**  
DeputyRecommender identifies deputies with similar profiles based on ideology, legislative activity, and resource allocation. A machine learning pipeline processes structured data, assigns feature importance, and calculates similarities using **cosine similarity**.

To enhance classification accuracy, the **GPT API** analyzes legislative propositions, extracting:

✔ **Ideological position** – from progressive to conservative  
✔ **Agenda category** – key topics such as economy, social policies, and governance  
✔ **Populist elements** – a score (0-1) indicating the level of populist appeal

For a more complete political landscape, GPT also classifies parties not present in the dataset. The ideological framework follows **"Uma Nova Classificação Ideológica dos Partidos Políticos Brasileiros"** (Bolognesi et al., 2023) [(DOI: 10.1590/dados.2023.66.2.303)](https://doi.org/10.1590/dados.2023.66.2.303).

* * *

🔎 **How It Works**

1️⃣ **Data Processing**

*   Infinite values replaced with `NaN`
*   Missing data handled:
    *   **Numerical**: filled with the median
    *   **Categorical**: filled with the most frequent value

2️⃣ **Feature Engineering**

*   Attributes categorized by importance: **high, medium, low**
*   Weights assigned to emphasize key factors in similarity calculations

3️⃣ **Preprocessing**

*   Standardization for numerical data
*   One-Hot Encoding for categorical values
*   Feature weighting to balance impact

4️⃣ **Similarity Calculation**

*   Cosine similarity measures proximity between deputies
*   Sparse matrix optimization ensures efficiency

5️⃣ **Personalized Recommendations**

*   Identifies deputies with the most similar profiles
*   Highlights key similarities for better interpretability

* * *

⚙ **Technology Stack**

🛠 **pandas, numpy** → data handling  
🛠 **scikit-learn** → preprocessing & similarity analysis  
🛠 **scipy.sparse** → optimized matrix operations  
🛠 **joblib** → model persistence  
🛠 **GPT API & Langhchain** → proposition & party classification


* * *

🎯 **Purpose**  
A powerful tool for identifying deputies with similar legislative behaviors. Essential for political analysis, policymaking insights, and strategic decision-making.

* * *

🇧🇷 **Sobre o Modelo**

🔎 **Recomendações Inteligentes para Análise Política**  
DeputyRecommender identifica deputados com perfis semelhantes com base em ideologia, atividade legislativa e uso de recursos. Um pipeline de aprendizado de máquina processa os dados estruturados, atribui pesos às características e calcula similaridades usando **cosseno de similaridade**.

Para aprimorar a precisão da classificação, a **API do GPT** analisa proposições legislativas e extrai:

✔ **Posição ideológica** – de progressista a conservador  
✔ **Categoria da agenda** – principais temas como economia, políticas sociais e governança  
✔ **Elementos populistas** – uma pontuação (0-1) que indica o apelo populista das proposições

Para um panorama político mais completo, o GPT também classifica partidos que não estavam no conjunto de dados original. A base ideológica segue **"Uma Nova Classificação Ideológica dos Partidos Políticos Brasileiros"** (Bolognesi et al., 2023) [(DOI: 10.1590/dados.2023.66.2.303)](https://doi.org/10.1590/dados.2023.66.2.303).

* * *

🔎 **Como Funciona**

1️⃣ **Processamento de Dados**

*   Valores infinitos substituídos por `NaN`
*   Tratamento de valores ausentes:
    *   **Numéricos**: preenchidos com a mediana
    *   **Categóricos**: preenchidos com o valor mais comum

2️⃣ **Engenharia de Atributos**

*   Características categorizadas por importância: **alta, média, baixa**
*   Pesos atribuídos para destacar os fatores mais relevantes na similaridade

3️⃣ **Pré-processamento**

*   Padronização de valores numéricos
*   One-Hot Encoding para variáveis categóricas
*   Aplicação de pesos para balanceamento

4️⃣ **Cálculo de Similaridade**

*   Medida de proximidade entre deputados com **cosseno de similaridade**
*   Matriz esparsa otimizada para eficiência

5️⃣ **Recomendações Personalizadas**

*   Identifica deputados com perfis mais semelhantes
*   Destaca as principais semelhanças para maior interpretabilidade

* * *

⚙ **Tecnologia Utilizada**

🛠 **pandas, numpy** → manipulação de dados  
🛠 **scikit-learn** → pré-processamento e análise de similaridade  
🛠 **scipy.sparse** → operações matriciais otimizadas  
🛠 **joblib** → persistência do modelo  
🛠 **API do GPT & Langchain** → classificação de proposições e partidos

* * *

🎯 **Objetivo**  
Uma ferramenta poderosa para identificar deputados com comportamentos legislativos semelhantes. Essencial para análise política, insights em formulação de políticas e tomada de decisão estratégica.
