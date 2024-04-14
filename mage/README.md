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
* [load_from_kaggle](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_loaders/load_from_kaggle.py)
  * Download Data from Kaggle;
  * Read the .csv by applying the schema;
  * Append the csv files with regard to their version;
  * Outputs 2 DataFrames, one for each player gender.
* [export_to_gcs](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_exporters/export_to_gcs.py)
  * Parametrized execution wrt variable 'male_dataset';
  * Loads data as csv file into GCS Bucket

## Transformation and Data Exporting into BigQuery

Since the Spark Transformation can be run locally or on the DataProc cluster, there are 2 pipelines that work accordingly to the transformation done.

### * Exporting Data by Triggering Spark Job on DataProc

### * Exporting Data produced by Local Spark Job
If the Spark Transformation was done locally, the transformed Data will be produced within a .parquet file and [this pipeline](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/tree/main/mage/fifa-processing/pipelines/local_pq_2_bigquery) is used to:
* [load_pq_local](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_loaders/load_pq_local.py) loading the transformed data in parquet format;
* [load_bigquery](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_exporters/load_bigquery.sql) export the transformed Data them into BigQuery.

