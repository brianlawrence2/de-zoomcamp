# Code for building the question 2 deployment

```zsh
prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs -n "Homework ETL"
prefect deployment apply etl_web_to_gcs-deployment.yaml
```

# Code for building the question 3 deployment

```zsh
prefect deployment build ./etl_gcs_to_bigquery.py:etl_parent_flow -n "Homework Q3 ETL"
prefect deployment apply etl_parent_flow-deployment.yaml
prefect agent start -q 'default'
```

# question 4 code
```zsh
prefect deployment build week_2/Homework/etl_web_to_gcs.py:etl_web_to_gcs -n "Github storage" -sb github/zoomcamp-github -o etl_web_to_gcs_github-deployment.yaml
prefect deployment apply etl_web_to_gcs_github-deployment.yaml
prefect agent start  --work-queue "default"
```