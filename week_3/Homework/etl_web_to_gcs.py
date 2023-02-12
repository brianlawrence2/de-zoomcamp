from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas dataframe"""
    # if randint(0,1) > 0 :
    #     raise Exception
    
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df.pickup_datetime = pd.to_datetime(df.pickup_datetime)
    df.dropOff_datetime = pd.to_datetime(df.dropOff_datetime)
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    
    return df

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write dataframe out local as a parquet file"""
    path = Path(f"data/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    return path

task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return    

@flow()
def etl_web_to_gcs(month: int, year: int) -> None:
     """The main etl function"""
     dataset_file = f"fhv_tripdata_{year}-{month:02}"
     dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
     
     df = fetch(dataset_url)
     df_clean = clean(df)
     path = write_local(df_clean, dataset_file)
     write_gcs(path)
     
@flow(log_prints=True)
def etl_parent_flow(months: list[int], year: int) -> None:
    for month in months:
        print(month)
        etl_web_to_gcs(month, year)
     
if __name__ == "__main__":
    months = list(range(1,13))
    year = 2019
    etl_parent_flow(months, year)