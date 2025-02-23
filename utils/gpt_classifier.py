from langchain_openai import OpenAI, ChatOpenAI
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from pydantic import BaseModel, Field
from enum import Enum
from typing import List
import pandas as pd
import json

import getpass
import os

if "OPENAI_API_KEY" not in os.environ:
    os.environ["OPENAI_API_KEY"] = getpass.getpass("Enter your OpenAI API key: ")

class AgendaCategory(str, Enum):
    ECONOMIC = "economic"
    SOCIAL = "social"
    ENVIRONMENTAL = "environmental"
    TECHNOLOGICAL = "technological"
    CULTURAL = "cultural"

class IdeologicalPosition(str, Enum):
    PROGRESSIVE = "progressive"
    MODERATE_PROGRESSIVE = "moderate_progressive"
    CENTRIST = "centrist"
    MODERATE_CONSERVATIVE = "moderate_conservative"
    CONSERVATIVE = "conservative"

class Classification(BaseModel):
    ideology: IdeologicalPosition = Field(
        description="The ideological position on the spectrum from progressive to conservative"
    )
    agenda_category: AgendaCategory = Field(
        description="The main category of the agenda (economic, social, environmental, etc.)"
    )
    populist_elements: float = Field(
        description="""Identify if this proposition have some large positive impact 
        on political opinion of the population. Its looks like the main interest of the proposition is to
        only to please a given spectrum of the electors? (0-1 scale)"""
    )

tagging_prompt = ChatPromptTemplate.from_template(
    """
    Extract the desired information from the following propositions of a deputy,
    summarize its agenda, ideology, and personality.

    Only extract the properties mentioned in the 'Classification' function.

    Note: 
        - The input is a list of propositions, each proposition is a string.
        - The output should be in pt-br
    
    Proposition list:
    {input}
    """
)

# LLM
llm = ChatOpenAI(temperature=0, model="gpt-4o-mini").with_structured_output(
    Classification
)


def classify_deputies(df):
    results = []
    for d_id in df.deputy_id.unique():
        df.loc[df.deputy_id == d_id, 'propositions_list'] = df.loc[df.deputy_id == d_id, 'propositions_list']
        inp = df.propositions_list[df.deputy_id == d_id]
        prompt = tagging_prompt.invoke({"input": inp})
        result = llm.invoke(prompt)
        results.append({
            "deputy_id": int(d_id),  # Convert int64 to int
            "classification": result.dict()
        })
    return json.dumps(results, ensure_ascii=False, indent=4)

df = pd.read_parquet('data/gold/deputies_consolidated_metrics_parquet').query('name == "André Janones"')
r = classify_deputies(df)
print(r)
