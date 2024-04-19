# Batch Processing fifa player dataset on Google Cloud
Wrap-up Data Engineering project meant to apply the concepts learned in the Data Engineering Zoomcamp Course, while developing a full-cycle Data Engineering project with Mage, GCS, BigQuery, PySpark and DataProc.

## Goal

The goal is to extract meaningful statistics from these Datam by comparing the different players information year by year. 
In particular most the queries written will highlight insights from each fifa version (or year) such as:
* the best players,
* change in the player values,
* the n best teams with regard to player score or player potential,
* the n best national teams.

## High level Architecture

![Project Architecture](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/images/architecture_dataproc.png)

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
This step can be executed both locally (with a PySpark installation + using Mage for loading), or on the cloud on a DataProc Cluster and it transforms the Data into a proper format and then loads the final table into BigQuery.

You can see more details on the dedicated [README](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/bigquery/README.md)

### 4. Visualisation

Finally let's see some of the results produced with Google Looker that show some insights of fifa players from 2017 to 2022.

![Fifa Players Statistics](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/images/fifa_stats.jpg)

![Fifa Players Teams and National Team Statistics](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/images/fifa_team_stats.jpg)

You can also go directly to the Looker Report [here](https://lookerstudio.google.com/reporting/c62af307-5e4b-4baf-a798-2ab39f905be8) and [here](https://lookerstudio.google.com/reporting/67a602f4-1d66-4820-aebc-2c7a1075a654).

Note that some of the data composing the tables and charts were added as Custom Queries in the Report Data and you can find them [here](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/bigquery/queries_partition.sql).

### Local Architecture Image

Since that for debugging the Transformation with Spark can be run locally, when doing so the architecture looks more like this.

![Project Architecture](https://github.com/lorenzomighie/batch-processing-fifa-dataset-on-gcp/blob/main/images/architecture_local.png)

### Credits

A huge thanks to [DataTalksClub](https://github.com/DataTalksClub) for delivery this high-quality course.


