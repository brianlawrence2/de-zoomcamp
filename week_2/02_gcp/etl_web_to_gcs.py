from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@flow()
def etl_web_to_gcs() -> None:
     """The main etl function""""
     color = "yellow"
     year = 2021
     dataset_file = f"{color}_tripdata_{year}-{month:02}"