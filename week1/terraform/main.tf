provider "google" {
  credentials = file(var.credentials)
  project     = "snappy-abode-449618-e1"
  region      = "us-east1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  delete_contents_on_destroy = true
  location = var.location
}
