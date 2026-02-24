# terraform/outputs.tf
# Outputs for resource identifiers needed by downstream tools and CI/CD.
# Retrieve after apply: terraform output -json

# ── AWS ───────────────────────────────────────────────────────────────────────

output "raw_data_lake_bucket" {
  description = "S3 bucket name for raw API response archives (partitioned Parquet/JSON)."
  value       = aws_s3_bucket.raw_data.bucket
}

output "processed_data_bucket" {
  description = "S3 bucket name for processed data (Delta Lake tables, dbt artefacts)."
  value       = aws_s3_bucket.processed_data.bucket
}

output "pipeline_iam_user_arn" {
  description = "ARN of the IAM user used by pipeline scripts for S3 read/write."
  value       = aws_iam_user.pipeline_user.arn
}

output "pipeline_iam_access_key_id" {
  description = "AWS access key ID for the pipeline IAM user. Store in .env / CI secrets."
  value       = aws_iam_access_key.pipeline_user.id
  sensitive   = true
}

output "pipeline_iam_secret_access_key" {
  description = "AWS secret access key for the pipeline IAM user. Store securely — not in code."
  value       = aws_iam_access_key.pipeline_user.secret
  sensitive   = true
}

# ── GCP / BigQuery ────────────────────────────────────────────────────────────

output "bigquery_warehouse_dataset" {
  description = "BigQuery dataset ID for the main econ warehouse (raw + mart tables)."
  value       = google_bigquery_dataset.econ_warehouse.dataset_id
}

output "bigquery_staging_dataset" {
  description = "BigQuery dataset ID for dbt staging models."
  value       = google_bigquery_dataset.econ_warehouse_staging.dataset_id
}

output "bigquery_marts_dataset" {
  description = "BigQuery dataset ID for dbt mart models (business-ready)."
  value       = google_bigquery_dataset.econ_warehouse_marts.dataset_id
}

output "bigquery_pipeline_service_account_email" {
  description = "Service account email used by dlt and dbt to write to BigQuery."
  value       = google_service_account.pipeline_sa.email
}

output "bigquery_pipeline_service_account_key" {
  description = "Base64-encoded JSON key for the pipeline service account. Set as GOOGLE_APPLICATION_CREDENTIALS."
  value       = google_service_account_key.pipeline_sa_key.private_key
  sensitive   = true
}
