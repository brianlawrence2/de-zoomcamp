from prefect import flow
from prefect.filesystems import GitHub

@flow(log_prints=True)
def github_flow(path: str = "de-zoomcamp"):
    github_block = GitHub.load("zoomcamp-github")
    github_block.get_directory(local_path="github/")
    
    from week_2.Homework.etl_web_to_gcs import etl_web_to_gcs
    etl_web_to_gcs()
    
    
if __name__ == "__main__":
    github_flow()