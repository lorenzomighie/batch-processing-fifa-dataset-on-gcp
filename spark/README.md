## Repository Files

In this folder you will find a couple of notebooks and the target script.

You can start by taking a look at the [spark_transformation_local_test](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/spark/spark_transformation_local_test.ipynb) file which contains the spark transformation on the data that were done locally.

This notebook is traduced into [this python script](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/spark/spark_transformation.py) that is parametrized to run both on local or in a Cluster.
You can set its argument when running like this:
```shell

# Local Run (outputs a parquet file)
python spark_transformation.py

# DataProc Run
python gs://mage-zoomcamp-bucket-lm/code/spark_transformation.py --local_run False --produce_parquet False --bucket "<bucket-name>" --dataset_name "<dataset-name>" --table_name "<table-name">
```

The [local_spark_test_queries](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/spark/local_spark_test_queries.ipynb) file instead contains some test queries done with spark and spark.Sql.

## Mage Pipelines

Since the Spark Transformation can be run locally or on the DataProc cluster, there are 2 pipelines that work accordingly to the transformation done.

### * Exporting Data by Triggering Spark Job on DataProc

### * Exporting Data produced by Local Spark Job
If the Spark Transformation was done locally, the transformed Data will be produced within a .parquet file and [this pipeline](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/tree/main/mage/fifa-processing/pipelines/local_pq_2_bigquery) is used to:
* [load_pq_local](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_loaders/load_pq_local.py) loading the transformed data in parquet format;
* [load_bigquery](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/mage/fifa-processing/data_exporters/load_bigquery.sql) export the transformed Data them into BigQuery.
