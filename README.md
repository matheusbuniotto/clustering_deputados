# Clusterização de Deputados Federais (2024)

An unsupervised learning and exploratory data project to discover behavioral patterns, voting alignments, and real affinities among Brazilian federal deputies using official legislative open data.

Developed as a capstone project for the **FIAP Machine Learning Engineering** specialization.

---

## 🏛️ What it does

Instead of relying on official party coalitions or political rhetoric, this project groups representatives based on **actual legislative behavior**:

1. **Automated Ingestion**: Scrapes and ingests voting rolls, session attendance, and authored propositions from the Brazilian Chamber of Deputies Open Data API.
2. **Feature Engineering**: Normalizes multi-dimensional legislative signals (attendance rates, voting discipline, legislative theme focus).
3. **Unsupervised Clustering**: Evaluates dimensionality reduction (PCA) and clustering algorithms (K-Means) to identify natural deputy clusters.
4. **Interactive Explorer**: A Streamlit application to visualize similarity clusters and inspect politician profiles.

---

## 📐 Architecture

![Architecture Flowchart](https://raw.githubusercontent.com/matheusbuniotto/clustering_deputados/refs/heads/main/docs/arch-flowchart.jpg)

---

## 🚀 Quick Start

### 1. Clone & Install Dependencies

```bash
git clone https://github.com/matheusbuniotto/clustering_deputados.git
cd clustering_deputados
pip install -r requirements.txt
```

### 2. Run the Streamlit Explorer

```bash
streamlit run app.py
```

---

## 💡 Acknowledgments

Inspired by Lauro Marques Vicari's exploratory data analysis on political behavior in Brazil.
