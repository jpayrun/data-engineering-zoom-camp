provider "google" {
  credentials = "../../keys/credentials.json"
  project     = "snappy-abode-449618-e1"
  region      = "us-east1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "snappy-abode-449618-e1-terra-bucket"
  location      = "US"
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
