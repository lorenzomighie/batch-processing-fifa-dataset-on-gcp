## Transformation

## Mage Pipelines

Since the Spark Transformation can be run locally or on the DataProc cluster, there are 2 pipelines that work accordingly to the transformation done.

### * Exporting Data by Triggering Spark Job on DataProc

### * Exporting Data produced by Local Spark Job
If the Spark Transformation was done locally, the transformed Data will be produced within a .parquet file and [this pipeline](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/tree/main/mage/fifa-processing/pipelines/local_pq_2_bigquery) is used to:
* [load_pq_local](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_loaders/load_pq_local.py) loading the transformed data in parquet format;
* [load_bigquery](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_exporters/load_bigquery.sql) export the transformed Data them into BigQuery.
