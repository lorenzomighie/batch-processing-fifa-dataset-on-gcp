# Batch processing of the fifa players dataset on Google Cloude
Wrap-up Data Engineering project meant to apply the concepts learned in the Data Engineering Zoomcamp Course, while developing a full-cycle Data Engineering project with Mage, GCS, BigQuery, PySpark and DataProc.

## Goal

The goal is to extract meaningful statistics from these Datam by comparing the different players information year by year. 
In particular most the queries written will highlight insights from each fifa version (or year) such as:
* the best players,
* change in the player values,
* the n best teams with regard to player score or player potential,
* the n best national teams.


## Steps

### 0. Setup Steps

#### a. Terraform Setup

The first step is provisioning the Infrastructure used in Google Cloud with Terraform for creating the following resources:
* Bucket
* BigQuery Dataset
* Dataproc cluster

More information in the [dedicated directory](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/tree/main/terraform).

#### b. Mage Setup

Mage.ai is used as the Workflow Orchestrator, setting it up is as easy as running `docker-compose up -d`.

In its [dedicated directory](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/tree/main/mage) you will find all the instruction to starting it up with the pipelines used for this project already embedded in it.
   
### 1. Extract and Load

In this step Mage pipeline is used for:
* Loading the data from Kaggle and doing some pre-processing
* Exporting the dataframes produced into a Google Cloud Storage Bucket (our Data Lake)

### 2. Data Exploration

This is a preliminary step to take into the consideration the type of the Queries that will be applied and hence deciding whether partitioning or clustering needs to take place. 

After noticing an improvement in performance when partitioning the data by the 'version' field, the final table will be partitioned accordingly.
More information on the dedicated [README](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/bigquery/README.md)

### 3. Transform (and Load into BigQuery)

Here come the transformation of the data with Spark.
This step can be executed both locally (with a PySpark installation), or on the cloud on a DataProc Cluster and it transforms the Data into a proper format and then loads the final table into BigQuery.

You can see more details on the dedicated [README](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/bigquery/README.md)

### 4. Visualisation


