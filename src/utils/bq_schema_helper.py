import pandas as pd
import numpy as np
import json
from typing import List, Optional
from google.cloud import bigquery
import logging

def align_df_to_bq_schema(df: pd.DataFrame, schema_fields: list) -> pd.DataFrame:
    """
    Align DataFrame column types with BigQuery schema.
    Adds warnings if dtypes don't match.
    """
    import pandas as pd

    bq_type_map = {
        "STRING": "object",
        "INTEGER": "Int64",   # nullable integer
        "FLOAT": "float64",
        "BOOLEAN": "boolean",
        "DATE": "datetime64[ns]",
        "TIMESTAMP": "datetime64[ns]",
    }

    for field in schema_fields:
        col = field.name
        bq_type = field.field_type.upper()

        if col not in df.columns:
            logging.debug(f"⚠️ Column {col} missing in DataFrame — filling with NaN")
            df[col] = pd.NA
            continue

        # Check if pandas dtype matches expected BigQuery type
        expected_dtype = bq_type_map.get(bq_type, None)
        actual_dtype = str(df[col].dtype)

        if expected_dtype and expected_dtype != actual_dtype:
            logging.debug(f"⚠️ Column {col} expected {bq_type} ({expected_dtype}) "
                  f"in BQ but got {actual_dtype} in pandas — coercing.")

        # Now coerce to correct dtype
        if bq_type == "STRING":
            df[col] = df[col].astype(str).replace("nan", None)
        elif bq_type == "INTEGER":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif bq_type == "FLOAT":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        elif bq_type == "BOOLEAN":
            df[col] = df[col].astype("boolean")
        elif bq_type in ("DATE", "TIMESTAMP"):
            df[col] = pd.to_datetime(df[col], errors="coerce")

             # Convert to ISO string for JSON serialization
            df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S").replace({pd.NaT: None})

    return df

def get_field_type(schema: List[bigquery.SchemaField], field_name: str) -> Optional[str]:
    """
    Look up the field type for a given column in a BigQuery schema.
    
    Args:
        schema (List[bigquery.SchemaField]): BigQuery schema list
        field_name (str): Column name to search for

    Returns:
        Optional[str]: Field type (e.g., "STRING", "FLOAT") or None if not found

    Usage:
        print(get_field_type(schema_fields, "arm_angle")) 

    """
    field = next((f for f in schema if f.name == field_name), None)
    return field.field_type if field else None  
