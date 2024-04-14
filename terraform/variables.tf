variable "credentials" {
  description = "My Credentials"
  default     = "/home/lorenz/batch-processing-fifa-dataset-on-gcp/terraform/svcaccount.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "diesel-air-394514"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-west6-a"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "diesel-air-394514"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "mage-zoomcamp-bucket-lm"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "gcs_storage_class" {
  description = "Project Name"
  default     = "diesel-air-394514"
}


variable "spark_cluster_name" {
  description = "Dataproc Cluster Name"
  default     = "dataproc-fifa"
}

