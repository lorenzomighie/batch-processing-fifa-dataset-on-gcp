# Batch processing of the fifa players dataset on Google Cloude
Wrap-up Data Engineering project meant to apply the concepts learned in the Data Engineering Zoomcamp Course, while developing a full-cycle Data Engineering project with Mage, GCS, BigQuery, PySpark and DataProc.

## Goal

The goal is to extract meaningful statistics from these Datam by comparing the different players information year by year. 
In particular most the queries written will highlight:
- The Best players year by yearm
- the best teams for each year, in both female and male football,
- the teams with the highest potential by each year and for each gender
- the increase in wage for the highest paid players year by year

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

In this step Mage pipeline is used to:
* Loading the data from Kaggle and doing some pre-processing
* Exporting the dataframes produced into a Google Cloud Storage Bucket (our Data Lake)

### 2. Data Exploration

Loading of raw data into BIGQUERY, SQL TESTS...
Explore transformation and queries, evaluate partitioning, show data warehousing

### 3. Transform (and Load into BigQuery)
Spark .... 

Explaination on data transformation, 

show sample queries

TODO SHOW DAG

THERE ARE 2 ways of running this 
- locally (run python script with no args)
- on dataproc (run on dataprocs with args)

### 4. Visualisation


