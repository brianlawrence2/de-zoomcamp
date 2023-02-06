# Code for building the question 2 deployment

```zsh
prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs -n "Homework ETL"
prefect deployment apply etl_web_to_gcs-deployment.yaml
```

