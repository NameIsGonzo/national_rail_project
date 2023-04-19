# Set up the bucket for historical data
resource "google_storage_bucket" "historical_data" {
  name          = "railscope_historical_data"
  location      = "US"
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
}

# Create the "checkpoints/" directory inside the historical data bucket
resource "google_storage_bucket_object" "historical_data_checkpoints" {
  name         = "checkpoints/"
  content_type = "application/x-www-form-urlencoded;charset=UTF-8"
  bucket       = google_storage_bucket.historical_data.id
}

# Set up the bucket for real-time data
resource "google_storage_bucket" "realtime_data" {
  name          = "railscope_realtime_data"
  location      = "US"
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
}

# Create the "checkpoints/" directory inside the real-time data bucket
resource "google_storage_bucket_object" "realtime_data_checkpoints" {
  name         = "checkpoints/"
  content_type = "application/x-www-form-urlencoded;charset=UTF-8"
  bucket       = google_storage_bucket.realtime_data.id
}

# Create the dataset for the project
resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "railscope"
  friendly_name               = "railscope"
  description                 = "This dataset holds all the tables for thhe project"
  location                    = "US"
}

