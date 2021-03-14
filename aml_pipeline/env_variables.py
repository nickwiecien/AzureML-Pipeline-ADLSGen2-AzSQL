"""Env dataclass to load and hold all environment variables
"""
from dataclasses import dataclass
import os
from typing import Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class Env:
    """Loads all environment variables into a predefined set of properties
    """

    # to load .env file into environment variables for local execution
    load_dotenv()
    workspace_name: Optional[str] = os.environ.get("WORKSPACE_NAME")
    resource_group: Optional[str] = os.environ.get("RESOURCE_GROUP")
    subscription_id: Optional[str] = os.environ.get("SUBSCRIPTION_ID")

    vm_size: Optional[str] = os.environ.get("AML_COMPUTE_CLUSTER_CPU_SKU", "STANDARD_D13_V2")
    compute_name: Optional[str] = os.environ.get("AML_COMPUTE_CLUSTER_NAME", "cpucluster")
    min_nodes: int = int(os.environ.get("AML_CLUSTER_MIN_NODES", 0))
    max_nodes: int = int(os.environ.get("AML_CLUSTER_MAX_NODES", 10))
    data_factory_name: Optional[str] = os.environ.get("DATA_FACTORY_NAME", "adfcompute")

    pipeline_name: Optional[str] = os.environ.get("PIPELINE_NAME")
    source_directory: Optional[str] = os.environ.get(
        "SOURCE_DIR"
    )
    sql_datastore_name: Optional[str] = os.environ.get("SQL_DATASTORE_NAME")
    adls_datastore_name: Optional[str] = os.environ.get("ADLS_DATASTORE_NAME")

    pipeline_name: Optional[str] = os.environ.get("PIPELINE_NAME")
