## Data Warehouse and BigQuery
- I copied Dockerfile and docker-compose.yaml from week_2 codes
- I created two dags to copy green and yellow files from web and convert to parquet format and then move to GCS (data_move_dag.py and data_move_green_dag.py, also ingest script ingest_script.py)
- I used the same code with sejal for crating external table and move data to native table gsc_2_bg_dag.py
- I added homework results as week_3_bigquery_homework.sql