## Terraform Set Up

First, make sure that you have the Google Cloud CLI installed, then, access it and authenticate.

```shell
# Refresh service-account's auth-token for this session
gcloud auth application-default login
```

## Terraform Execution

Now you can run Terraform commands:

```
# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=<your-gcp-project-id>" -var="gcs_bucket_name=<bucket-name>"
```

If the plan look good you are ready to apply to GCP. This process will provide
* a bucket with regard to the variable *gcs_bucket_name*
* a bigquery dataset with regard to the variable *bq_dataset_name* 
* a datproc cluster with regard to the variable *spark_cluster_name* 

```shell
# Create new infra
terraform apply -var="project=<your-gcp-project-id>"
```

```shell
# Delete infrastructure
terraform destroy
```
