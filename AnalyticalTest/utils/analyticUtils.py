import os
from dotenv import load_dotenv
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase

def getDataFrameFor1InchFlow(blockchain, dst_token_symbol, src_token_symbol):
    load_dotenv()

    dune_api_key = os.getenv('DUNE_API_KEY')
    if not dune_api_key:
        raise ValueError("DUNE_API_KEY is not set in the environment variables")

    query = QueryBase(
        name="Sample Query",
        query_id=4326520,
        params=[
            QueryParameter.text_type(name="blockchain", value=blockchain),
            QueryParameter.text_type(name="src_token_symbol", value=src_token_symbol),
            QueryParameter.text_type(name="dst_token_symbol", value=dst_token_symbol),
        ],
    )
    print("Results available at", query.url())

    dune = DuneClient.from_env()
    results_df = dune.run_query_dataframe(query)
    return results_df

def getDataFrameFor1InchIntentFlow(blockchain, dst_token_symbol, src_token_symbol):
    load_dotenv()

    dune_api_key = os.getenv('DUNE_API_KEY')
    if not dune_api_key:
        raise ValueError("DUNE_API_KEY is not set in the environment variables")

    query = QueryBase(
        name="Sample Query",
        query_id=4326581,
        params=[
            QueryParameter.text_type(name="blockchain", value=blockchain),
            QueryParameter.text_type(name="src_token_symbol", value=src_token_symbol),
            QueryParameter.text_type(name="dst_token_symbol", value=dst_token_symbol),
        ],
    )
    print("Results available at", query.url())

    dune = DuneClient.from_env()
    results_df = dune.run_query_dataframe(query)
    return results_df

def getAssetsWithMostTradesOn1InchFusion():
    load_dotenv()

    dune_api_key = os.getenv('DUNE_API_KEY')
    if not dune_api_key:
        raise ValueError("DUNE_API_KEY is not set in the environment variables")

    query = QueryBase(
        name="Query for most most traded assets",
        query_id=4323741,
        params=[],
    )
    print("Results available at", query.url())

    dune = DuneClient.from_env()
    results_df = dune.run_query_dataframe(query)
    return results_df