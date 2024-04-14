# Mage Setup

Here is the Mage setup with the pipelines for the data ingestion.

1. set the var PROJECT_NAME into a .env file
2. login into Kaggle and create your api-token, donwload it and write its path into the variable KAGGLE_PATH in the .env file
3. get your google service account json file and place it inside this folder by naming it as *svcaccount.json*
4. Make sure you have installed docker and docker-compose
5. Make sure port 6789 is available on your machine
6. Sping Mage up by running `docker-compose up -d` within this folder

## Data Loading and Exporting
The pipeline blocks are located within the fifa-processing directory, however here is a quick
lookup to the [kaggle_2_gcs pipeline](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/pipelines/kaggle_2_gcs/metadata.yaml) blocks:
- [load_from_kaggle](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_loaders/load_from_kaggle.py)
- [select_colums](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/transformers/select_columns.py)
- [export_to_gcs](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_exporters/export_to_gcs.py)

## Data Exporting into BigQuery

## Data Loading from Local and Exporting into BigQuery vs Run DataProc job


While the [gcs_2_bigquery pipeline](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/pipelines/gcs_2_bigquery/metadata.yaml)  blocks for processing the data from gcs and loading them to bigquery are the following:
- [load_from_gcs](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_loaders/load_from_gcs.py)
- [export_to_bigquery](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_exporters/export_to_bigquery.sql)
