import numpy as np
import pandas as pd
from cassandra.cluster import Cluster
from dagster import op, Out, In, get_dagster_logger
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from pymongo import MongoClient, errors
from sqlalchemy import create_engine, exc, VARCHAR, DECIMAL, INT, text
from sqlalchemy.pool import NullPool
from sqlalchemy.types import *
import pandas.io.sql as sqlio


logger = get_dagster_logger()

@op(
    ins={"start": In(bool)},
    out=Out([])
)

def transform_cost_of_living(start):
    return []

@op(
    ins={"start": In(bool)},
    out=Out([])
)

def transform_quality_of_life(start):
    return []



@op(
    ins={"start": In(bool)},
    out=Out([])
)

def transform_unemployment(start):
    return []


@op(
    ins={'dataframe1': [], 'dataframe2': [], 'dataframe3': []},
    out=Out(pd.DataFrame)
)

def join(dataframe1, dataframe2, dataframe3) -> pd.DataFrame:
    
    return []

    

@op(
    ins={'dataframe_merged': []},
    out=Out(bool)
)

def load(merged_dataframe):
    try:
        # Return the number of rows inserted
        return []
    
    # Trap and handle any relevant errors
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False