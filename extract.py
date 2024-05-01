import json
import pandas as pd
from cassandra.cluster import Cluster
from dagster import op, Out, In, get_dagster_logger
from datetime import date, datetime
from pymongo import MongoClient, errors
import numpy as np



logger = get_dagster_logger()


@op(
    out=Out(bool)
)

def extract_unemployment() -> bool:
    return []

@op(
    out=Out(bool)
)

def extract_quality_of_life() -> bool:
    return []


@op(
    out=Out(bool)
)

def extract_cost_of_living() -> bool:
    # Return the result flag
    return []