Here is the Mage setup with the pipelines for the data ingestion.

Set up the Mage environment with docker-compose up -d

remember to set you .env variable, to add svc account in the home and the kaggle api key.

Withing the folder fifa-processing all pipelines can be seen, however for a quick
lookup to the [pipeline](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/pipelines/kaggle_2_gcs/metadata.yaml) blocks ingesting from kaggle and loading to the gcs data lake go here
https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_loaders/load_from_kaggle.py
https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/transformers/select_columns.py

While the [pipeline](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/pipelines/gcs_2_bigquery/metadata.yaml)  blocks for processing the data from gcs and loading them to bigquery are the following...
https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_loaders/load_from_gcs.py
https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_exporters/export_to_bigquery.sql
