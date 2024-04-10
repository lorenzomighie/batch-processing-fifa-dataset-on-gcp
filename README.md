# batch-processing-fifa-dataset-on-gcp
Wrap-up Data Engineering project meant to apply the concepts learned in the Data Engineering Zoomcamp Course, while developing a full-cycle Data Engineering project with Mage, GCS, BigQuery and Spark

Aim is visualizing the n best teams for player ranking and the n best team for player potential, in each year for each league.


Steps

0) Google Cloud Environment setup with Terraform
   
0b) Mage Setup

2) Ingestion pipeline in Mage
  a) load data from kaggle
  b) transform data
  c) load data into a Data Lake (a GCS Bucket)

3) Loading data into Data Warehouse
  a) load data from gcs
  b) data transformation ???? EXPLAINATION : DATA PARTITIONING is not useful as table size < 1 GB, show example of the query that will be done, do clustering
  c) load data into a BigQuery Table

2b) Make some sample queries and show performances

3) Spark Transformation on DataProc

4) Dashboard and Results
