
variable "credentials" {
    description = "My Credentials"
    default = "~/.google/credentials/google_credentials.json"
}

variable "location" {
    description = "Project location"
    default = "US"

}

variable "bq_dataset_name" {
    description = "My BigQuery Dataset Name"
    default = "demo_dataset"

}

variable "gcs_bucket_name" {
    description = "My Storage bucket name"
    default = "snappy-abode-449618-e1-terra-bucket"

}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}