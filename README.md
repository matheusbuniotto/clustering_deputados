<div align="left" style="position: relative;">
<img src="https://raw.githubusercontent.com/PKief/vscode-material-icon-theme/ec559a9f6bfd399b82bb44393651661b08aaf7ba/icons/folder-markdown-open.svg" align="right" width="30%" style="margin: -20px 0 0 20px;">
<h1>CLUSTERING_DEPUTADOS</h1>
<p align="left">
	<em>Discover similarities between deputies in 2024!</em>
</p>
<p align="left">
	<img src="https://img.shields.io/github/license/matheusbuniotto/clustering_deputados?style=default&logo=opensourceinitiative&logoColor=white&color=6b00ff" alt="license">
	<img src="https://img.shields.io/github/last-commit/matheusbuniotto/clustering_deputados?style=default&logo=git&logoColor=white&color=6b00ff" alt="last-commit">
	<img src="https://img.shields.io/github/languages/top/matheusbuniotto/clustering_deputados?style=default&color=6b00ff" alt="repo-top-language">
	<img src="https://img.shields.io/github/languages/count/matheusbuniotto/clustering_deputados?style=default&color=6b00ff" alt="repo-language-count">
</p>
<p align="left"><!-- default option, no dependency badges. -->
</p>
<p align="left">
	<!-- default option, no dependency badges. -->
</p>
</div>
<br clear="right">

## 🔗 Table of Contents

- [📍 Overview](#-overview)
- [👾 Features](#-features)
- [📁 Project Structure](#-project-structure)
  - [📂 Project Index](#-project-index)
- [🚀 DIY](#-getting-started)
  - [☑️ Prerequisites](#-prerequisites)
  - [⚙️ Installation](#-installation)
- [🙌 Acknowledgments](#-acknowledgments)

---

## 📍 Overview

This project provide a way to compare political deputies using AI and ML. It preprocesses legislative data to identify similarities among deputies, offering personalized recommendations of similarity through an intuitive interface. This tool is part of a personal project for FIAP Machine Learing Engineer Specialization, and aim to provide voters a way  to seek deeper insights into deputies' alignments and legislative behaviors, enhancing transparency and informed decision-making in the political landscape. The project uses the 2024 data from the goverment API and webscrape relevant date (attendance) and enhance this data usign AI (GPT + Langchain). While the project uses 2024, if running locally, you can choose the range or a given year to run this analysis and pull the data from the API using the provided script.

---

## 👾 Features

|      | Feature         | Summary       |
| :--- | :---:           | :---          |
| ⚙️  | **Architecture**  | <ul><li>Utilizes a modular approach with separate components for model handling (`model.py`), user interface (`app.py`), and data processing (`utils/data_processing.py`, `utils/gpt_classifier.py`).</li><li>Employs `<Streamlit>` for interactive front-end capabilities.</li><li>Integrates machine learning and data visualization to provide deputy recommendations and comparisons.</li> <li> Uses Medallion Architecture for data storage, using raw, bronze, silver and gold to store data. The data is stored locally for easier comsumption if needed in the parquet, csv and db format. While the data is provided locally, the main project uses S3 to store the data in parquet format partioned if needed. </li> </ul> 
| 🔩 | **Code Quality**  | <ul><li>Code is organized into distinct modules for different functionalities, enhancing maintainability.</li><li>Adheres to modern coding practices with clear separation of concerns among components.</li></ul> |
| 🔌 | **Integrations**  | <ul><li>Integrates with `<Streamlit>` for web-based interfaces.</li><li>Uses `<Langchain>` for GPT API consumption.</li><li>Employs various data science libraries like `<numpy>`, `<pandas>`, and `<scikit-learn>` for data manipulation and machine learning tasks.</li></ul> |
| 🧩 | **Modularity**    | <ul><li>Highly modular design with clear separation into data handling, processing, and user interface modules.</li><li>Enables easy scalability and maintenance by isolating changes to specific areas of the application.</li><li>Modular approach supports potential expansion with additional features or integrations.</li></ul> |
| ⚡️  | **Performance**   | <ul><li>Leverages efficient data processing libraries like `<duckdb>` to handle complex datasets.</li><li>Utilizes `<scikit-learn>` for performance-optimized machine learning operations storing the model to use inference.</li><li>Performance considerations are critical in the processing and classification tasks to handle potentially large volumes of legislative data.</li></ul> |

---

## 📁 Project Structure

```sh
└── clustering_deputados/
    ├── README.md
    ├── app.py
    ├── data
    │   ├── bronze
    │   │   ├── dim_deputies.csv
    │   │   ├── fact_attendace.csv
    │   │   ├── fact_expenses.csv
    │   │   ├── fact_propositions.csv
    │   │   ├── ingestion.py
    │   │   ├── partidos_classificados.csv
    │   │   └── sources.json
    │   ├── deputies_db.db
    │   ├── enriched_df.csv
    │   ├── get_data.py
    │   ├── gold
    │   │   ├── classified_deputies.json
    │   │   ├── classified_deputies.parquet
    │   │   ├── deputies_consolidated_metrics.sql
    │   │   ├── deputies_consolidated_metrics_parquet
    │   │   └── ingestion.py
    │   ├── party_ideology_map.json
    │   ├── raw
    │   │   ├── attendance_2024_20250219.csv
    │   │   ├── class_partidos.csv
    │   │   ├── deputies_2024_20250219.csv
    │   │   ├── expense_2024_20250219.csv
    │   │   └── proposition_2024_20250219.csv
    │   ├── s3_upload.py
    │   └── silver
    │       ├── deputies_attendance.sql
    │       ├── deputies_attendance_parquet
    │       ├── deputies_expenses.sql
    │       ├── deputies_expenses_parquet
    │       ├── deputies_party_classification.sql
    │       ├── deputies_party_classification_parquet
    │       ├── deputies_propositions.sql
    │       ├── deputies_propositions_parquet
    │       └── ingestion.py
    ├── docs
    │   └── about_the_model.md
    ├── model
    │   ├── data.pkl
    │   └── similarity.pkl
    ├── model.py
    ├── requirements.txt
    └── utils
        ├── data_processing.py
        └── gpt_classifier.py
```


### 📂 Project Index
<details open>
	<summary><b><code>CLUSTERING_DEPUTADOS/</code></b></summary>
	<details> <!-- __root__ Submodule -->
		<summary><b>__root__</b></summary>
		<blockquote>
			<table>
			<tr>
				<td><b><a href='https://github.com/matheusbuniotto/clustering_deputados/blob/master/model.py'>model.py</a></b></td>
				<td>- Model.py establishes a DeputyRecommender system that preprocesses legislative data, computes similarity scores among deputies using cosine similarity score, and provides recommendations based on these similarities<br>- It handles data cleaning, feature preparation, and model persistence, enabling efficient retrieval and comparison of deputies to suggest matches.</td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/matheusbuniotto/clustering_deputados/blob/master/app.py'>app.py</a></b></td>
				<td>- App.py serves as the interactive front-end component of a Deputy Recommender System, utilizing Streamlit to provide a user interface where users can select political deputies and receive personalized recommendations<br>- It integrates data visualization and machine learning models to display deputy profiles, compare metrics, and suggest similar deputies based on selected criteria.</td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/matheusbuniotto/clustering_deputados/blob/master/requirements.txt'>requirements.txt</a></b></td>
				<td>- Manages the installation of Python libraries essential for the project's operation, ensuring compatibility and functionality across various modules<br>- It specifies exact versions of dependencies like Flask, numpy, and pandas, which are crucial for web services, data manipulation, and analysis tasks within the broader application architecture. The boto3 librarie isnt present in the requirments, but it should be installed if working with AWS S3 bucekts. </td>
			</tr>
			</table>
		</blockquote>
	</details>
	<details> <!-- utils Submodule -->
		<summary><b>utils</b></summary>
		<blockquote>
			<table>
			<tr>
				<td><b><a href='https://github.com/matheusbuniotto/clustering_deputados/blob/master/utils/data_processing.py'>data_processing.py</a></b></td>
				<td>- Processes and enriches parliamentary data by reading metrics from API and webscraping, saving it to an S3 bucket, classifying and enhance data using a GPT-based model, and performing feature engineering and recommendation<br>- The script merges datasets, applies classifications, calculates metrics, and exports the enhanced dataset for further analysis. It also build a streamlit app for easy consumption of recommendations. </td>
			</tr>
			<tr>
				<td><b><a href='https://github.com/matheusbuniotto/clustering_deputados/blob/master/utils/gpt_classifier.py'>gpt_classifier.py</a></b></td>
				<td>- Classifies Brazilian political deputies and parties based on their ideological positions and agenda categories using a language model<br>- It processes input data, applies a classification schema, and saves the results in multiple formats, providing recommendations of similar deputies using cosine similarity.</td>
			</tr>
			</table>
		</blockquote>
	</details>
</details>

---
## 🚀 DIY

### ☑️ Prerequisites

Before getting started with clustering_deputados, ensure your runtime environment meets the following requirements:

- **Programming Language:** Python
- **Package Manager:** Pip


### ⚙️ Installation

**Build from git:**

1. Clone the clustering_deputados repository:
```sh
❯ git clone https://github.com/matheusbuniotto/clustering_deputados/
```

2. Navigate to the project directory:
```sh
❯ cd clustering_deputados
```

3. Install the project dependencies:


**Using `pip`** &nbsp; [<img align="center" src="https://img.shields.io/badge/Pip-3776AB.svg?style={badge_style}&logo=pypi&logoColor=white" />](https://pypi.org/project/pip/)

```sh
❯ pip install -r requirements.txt
```

## 🙌 Acknowledgments

- This work was inspired by this analysis

---

### Done:
S3 
![image](https://github.com/user-attachments/assets/e21f95b0-5954-4325-9027-8366457b44a6)

![image](https://github.com/user-attachments/assets/2922ec9f-eb7d-4217-82d6-17ae392b7f99)

