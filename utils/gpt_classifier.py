import getpass
import json
import os
import logging
from enum import Enum
from typing import List

import pandas as pd
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

# Constants for file paths
INPUT_DATA_PATH = "../data/gold/deputies_consolidated_metrics_parquet"
OUTPUT_PARQUET_PATH = "../data/gold/classified_deputies.parquet"
OUTPUT_JSON_PATH = "../data/gold/classified_deputies.json"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AgendaCategory(str, Enum):
    """Categories for political agendas."""
    ECONOMIC = "economic"
    SOCIAL = "social"
    ENVIRONMENTAL = "environmental"
    TECHNOLOGICAL = "technological"
    CULTURAL = "cultural"


class IdeologicalPosition(str, Enum):
    """Political ideology spectrum positions."""
    PROGRESSIVE = "progressive"
    MODERATE_PROGRESSIVE = "moderate_progressive"
    CENTRIST = "centrist"
    MODERATE_CONSERVATIVE = "moderate_conservative"
    CONSERVATIVE = "conservative"


class Classification(BaseModel):
    """Classification schema for political propositions."""
    ideology: IdeologicalPosition = Field(
        description="Ideological position from progressive to conservative"
    )
    agenda_category: AgendaCategory = Field(
        description="Main category of the agenda (economic, social, etc.)"
    )
    populist_elements: float = Field(
        description=(
            "Measure of populist appeal in propositions (0-1 scale). "
            "Assesses if primary purpose is to please specific voter segments."
        )
    )


TAGGING_PROMPT = ChatPromptTemplate.from_template(
    """
    Analyze political propositions and classify them using these criteria:
    1. Ideological position
    2. Agenda category
    3. Populist elements (0-1 score)

    Focus strictly on the Classification schema properties.
    
    Requirements:
    - Input: List of proposition strings
    - Output: Brazilian Portuguese
    
    Propositions:
    {input}
    """
)


def initialize_llm():
    """Configure and return the language model."""
    return ChatOpenAI(temperature=0.1, model="gpt-4o-mini").with_structured_output(Classification)


def process_deputy_propositions(deputy_df: pd.DataFrame, llm: ChatOpenAI) -> dict:
    """Process propositions for a single deputy."""
    propositions = deputy_df["propositions_list"].iloc[0]
    prompt = TAGGING_PROMPT.invoke({"input": propositions})
    result = llm.invoke(prompt)
    return {
        "deputy_id": int(deputy_df.deputy_id),
        "classification": result.model_dump()
    }


def classify_deputies_batch(df: pd.DataFrame, batch_size: int = 50) -> List[dict]:
    """Process deputies in batches with error handling."""
    llm = initialize_llm()
    results = []
    deputy_groups = df.groupby("deputy_id")

    for deputy_id, group in deputy_groups:
        try:
            logging.info(f"Processing deputy {deputy_id}")
            results.append(process_deputy_propositions(group, llm))
        except Exception as e:
            logging.error(f"Error processing deputy {deputy_id}: {str(e)}")
    
    return results


def save_results(results: List[dict]) -> None:
    """Save classification results in multiple formats."""
    json_data = json.dumps(results, ensure_ascii=False, indent=4)
    
    pd.DataFrame(results).to_parquet(
        OUTPUT_PARQUET_PATH,
        index=False
    )
    
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        f.write(json_data)

def classify_party(party_name):
    if "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = getpass.getpass("OpenAI API Key: ")
    """
    Classifies a Brazilian political party by ideology.
    First checks the dictionary, then queries OpenAI if missing.
    """
    
    classification_prompt = ChatPromptTemplate.from_template("""
    Return the ideological position of a given Brazillian party.
    Answer should contain only one option of the following: 
        - 'Esquerda'
        - 'Centro-esquerda'
        - 'Centro'
        - 'Centro-direita'
        - 'Direita'
        - 'Extrema-direita'
        
    # Examples
        Party: PT
        Output: Esquerda
        
        Party: PL
        Output: Direita    

        Party: PCdoB
        Output: Extrema-esquerda
        
        Party: Patriota
        Output: Extrema-direita
    

    Party: {party_name}
    Note: you will be punished if return something different from the options above"
    """)
    llm = ChatOpenAI(model="gpt-4o-mini")
    
    # Format the prompt
    prompt = classification_prompt.format(party_name=party_name)
    
    response = llm.predict(prompt)

    # Store the result in dictionary to avoid repeated API calls
    classification = response.strip()

    return classification

def main() -> None:
    """Main execution workflow."""
    if "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = getpass.getpass("OpenAI API Key: ")

    logging.info("Reading input data")
    deputies_df = pd.read_parquet(INPUT_DATA_PATH)
    logging.info("Classifying deputies")
    classifications = classify_deputies_batch(deputies_df.query('proposition_count > 0'))
    logging.info("Saving results")
    save_results(classifications)

    logging.info(f"Results saved to:\n- {OUTPUT_PARQUET_PATH}\n- {OUTPUT_JSON_PATH}")


if __name__ == "__main__":
    main()
    


