import streamlit as st
import pandas as pd
from model import DeputyRecommender
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from sklearn.preprocessing import StandardScaler

# Set the page configuration to wide
st.set_page_config(layout="wide")

# Load the deputies data to populate the dropdown menu
data_path = 'data/enriched_df.csv'
df = pd.read_csv(data_path, encoding='utf-8')

# Initialize the recommender
recommender = DeputyRecommender(data_path)

# Function to load and display the Markdown file
def load_markdown(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    st.markdown(content)

# Streamlit app
st.title('Deputy Recommender System')

# Sidebar navigation with selectbox
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Go to", ["Recommender", "Model Explanation"])

if page == "Recommender":
    # Dropdown menu for selecting a deputy
    deputy_name = st.selectbox('Select a Deputy:', df['name'].unique())

    # Number of recommendations to display
    top_n = st.slider('Number of Recommendations:', 1, 5, 3)

    # Button to get recommendations
    if st.button('Get Recommendations'):
        # Get the deputy_id based on the selected deputy_name
        deputy_id = df.loc[df['name'] == deputy_name, 'deputy_id'].values[0]
        
        try:
            recommendations = recommender.recommend_by_id(deputy_id, top_n)
            st.write(f"Recommendations for {deputy_name}:")
            
            # Display the main deputy's photo and data
            cols = st.columns([1, 3])
            with cols[0]:
                main_deputy_photo_url = df.loc[df['deputy_id'] == deputy_id, 'photo_url'].values[0]
                st.image(main_deputy_photo_url, caption=deputy_name, width=150)
            
            with cols[1]:
                # Create a dashboard view for the main deputy
                st.subheader("Deputy information for the year 2024")
                st.write("*Ideology, populist elements (score 0 to 1) and agenda was generated using Zero Shot GPT API to summarize the propositions of a given deputy.")
                st.write(f"**Name:** {deputy_name}")
                st.write(f"**ID:** {deputy_id}")
                
                # Plot categorical features
                categorical_features = ['party_classification', 'ideology', 'party', 'state']
                for feature in categorical_features:
                    deputy_value = df.loc[df['deputy_id'] == deputy_id, feature].values[0]
                    st.write(f"**{feature.replace('_', ' ').title()}:** {deputy_value}")
            
            st.subheader("Deputy Metrics in Comparison")

            # Plot numerical features compared to the dataset mean and median
            numerical_features = ['attendance_rate', 'cost_per_proposition', 'populist_elements', 'proposition_count', 'total_expenses', 'mean_expend_per_document']
            cols = st.columns(2)
            
            for i, feature in enumerate(numerical_features):
                deputy_value = df.loc[df['deputy_id'] == deputy_id, feature].values[0]
                # Handle NaN values in mean/median calculations
                clean_series = df[feature].replace([np.inf, -np.inf], np.nan).dropna()
                mean_value = clean_series.mean() if not clean_series.empty else 0
                median_value = clean_series.median() if not clean_series.empty else 0
                
                # Handle division by zero in percentage calculation
                if mean_value != 0:
                    percentage_diff = ((deputy_value - mean_value) / mean_value) * 100
                else:
                    percentage_diff = 0
                
                cols[i % 2].write(f"**{feature.replace('_', ' ').title()}:** "
                                  f"{deputy_value:.2f} | Mean: {mean_value:.2f} | "
                                  f"Median: {median_value:.2f} | "
                                  f"Diff: {percentage_diff:+.1f}%")
                
                # Collect values for the recommendations
                rec_values = [df.loc[df['deputy_id'] == rec['deputy_id'], feature].values[0] for rec in recommendations['similar_deputies']]
                fig = px.bar(
                    x=['Deputy', 'Mean', 'Median'] + [rec['name'] for rec in recommendations['similar_deputies']], 
                    y=[deputy_value, mean_value, median_value] + rec_values, 
                    labels={'x': '', 'y': feature.replace('_', ' ').title()},
                    color_discrete_sequence=['blue', 'lightblue', 'lightblue'] + ['gray'] * len(recommendations['similar_deputies'])
                )
                fig.update_traces(marker=dict(color=['blue', 'lightblue', 'lightblue'] + ['gray'] * len(recommendations['similar_deputies'])))
                cols[i % 2].plotly_chart(fig, use_container_width=True)

            
            # Plot a row of recommendations with improved text display
            st.subheader("Recommendations")
            cols = st.columns(top_n)
            for i, rec in enumerate(recommendations['similar_deputies']):
                with cols[i]:
                    rec_photo_url = df.loc[df['deputy_id'] == rec['deputy_id'], 'photo_url'].values[0]
                    st.image(rec_photo_url, caption=rec['name'], width=100)
                    st.markdown(f"**Name:** {rec['name']}")
                    st.markdown(f"**Similarity Score:** {rec['similarity_score']}")
                    st.markdown(f"**Key Similarities:** {rec['key_similarities']}")
                    st.markdown(f"**Most Similar Fields:** {rec['most_similar_fields']}")
            
            # Display the deputy and recommendations data
            st.write("Deputy and Recommendations Data")
            st.write(df[df['deputy_id'] == deputy_id])
            st.write(df[df['deputy_id'].isin([rec['deputy_id'] for rec in recommendations['similar_deputies']])])
        
        except ValueError as e:
            st.error(f"Error: {e}")
        except Exception as e:
            st.error(f"Internal Error:: {e}")

# elif page == "Chat LLM":
#     st.title("TO-DO Chat (LLM) with data")
#     st.write("Todo: Precisando implementar algo com langchain +  gpt-4o-mini.")
    
#     # Placeholder for chat interface
#     chat_input = st.text_input("Ask somethign: ", "")
#     if chat_input:
#         # Placeholder for LLM response
#         st.write("TO-DO: add a model with RAG on the dataset")

elif page == "Model Explanation":
    st.title("Model Explanation")
    load_markdown('docs/about_the_model.md')