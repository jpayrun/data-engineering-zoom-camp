
variable "credentials" {
    description = "My Credentials"
    default = "~/.google/credentials/google_credentials.json"
}

variable "project" {
    description = "Project description"
    default = "snappy-abode-449618-e1"
}

variable "location" {
    description = "Project location"
    default = "US"

}

variable "bq_dataset_name" {
    description = "My BigQuery Dataset Name"
    default = "trips_data_all"

}

variable "gcs_bucket_name" {
    description = "My Storage bucket name"
    default = "snappy-abode-449618-e1-terra-bucket"

}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}