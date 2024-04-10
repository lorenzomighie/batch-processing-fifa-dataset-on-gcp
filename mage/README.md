Here is the Mage setup with the pipelines for the data ingestion.

Set up the Mage environment with docker-compose up -d

remember to set you .env variable, to add svc account in the home and the kaggle api key.

Withing the folder fifa-processing all pipelines can be seen, however for a quick
lookup to the pipeline blocks ingesting from kaggle and loading to the gcs data lake go here
url
url
url

While the pipeline blocks for processing the data from gcs and loading them to bigquery are the following...
