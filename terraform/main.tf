# terraform/main.tf
# ============================================================
# Econ Data Pipeline — Infrastructure as Code
# Provisions: AWS S3 buckets, GCP BigQuery datasets,
#             and supporting IAM/service accounts
# ============================================================

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Remote state (uncomment for team use)
  # backend "s3" {
  #   bucket = "econ-pipeline-tfstate"
  #   key    = "econ-data-pipeline/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

# ── Providers ─────────────────────────────────────────────────────────────────

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "econ-data-pipeline"
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = "2Pure1"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# ── AWS S3 — Raw Data Lake ────────────────────────────────────────────────────

resource "aws_s3_bucket" "raw_data" {
  bucket = "${var.project_name}-raw-data-${var.environment}"
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "raw-data-lifecycle"
    status = "Enabled"


    filter {}

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket                  = aws_s3_bucket.raw_data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── AWS S3 — Processed Data ───────────────────────────────────────────────────

resource "aws_s3_bucket" "processed_data" {
  bucket = "${var.project_name}-processed-${var.environment}"
}

resource "aws_s3_bucket_versioning" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "processed_data" {
  bucket                  = aws_s3_bucket.processed_data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── IAM — Pipeline User ───────────────────────────────────────────────────────

resource "aws_iam_user" "pipeline_user" {
  name = "${var.project_name}-pipeline-user-${var.environment}"
}

resource "aws_iam_access_key" "pipeline_user" {
  user = aws_iam_user.pipeline_user.name
}

resource "aws_iam_user_policy" "pipeline_s3_policy" {
  name = "econ-pipeline-s3-policy"
  user = aws_iam_user.pipeline_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*",
          aws_s3_bucket.processed_data.arn,
          "${aws_s3_bucket.processed_data.arn}/*",
        ]
      }
    ]
  })
}

# ── GCP BigQuery ──────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "econ_warehouse" {
  dataset_id    = "econ_warehouse_${var.environment}"
  friendly_name = "Econ Warehouse (${var.environment})"
  description   = "Macroeconomic data warehouse for ML forecasting"
  location      = var.bq_location

  labels = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }

  delete_contents_on_destroy = var.environment == "dev" ? true : false
}

resource "google_bigquery_dataset" "econ_warehouse_staging" {
  dataset_id    = "econ_staging_${var.environment}"
  friendly_name = "Econ Staging (${var.environment})"
  description   = "dbt staging models"
  location      = var.bq_location

  labels = {
    project     = var.project_name
    environment = var.environment
    layer       = "staging"
  }
}

resource "google_bigquery_dataset" "econ_warehouse_marts" {
  dataset_id    = "econ_marts_${var.environment}"
  friendly_name = "Econ Marts (${var.environment})"
  description   = "dbt mart models — business-ready"
  location      = var.bq_location

  labels = {
    project     = var.project_name
    environment = var.environment
    layer       = "marts"
  }
}

# ── GCP Service Account ───────────────────────────────────────────────────────

resource "google_service_account" "pipeline_sa" {
  account_id   = "econ-pipeline-sa-${var.environment}"
  display_name = "Econ Pipeline Service Account"
  description  = "Used by dlt and dbt to write to BigQuery"
}

resource "google_bigquery_dataset_iam_member" "pipeline_sa_warehouse" {
  dataset_id = google_bigquery_dataset.econ_warehouse.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_sa_job_user" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
}
