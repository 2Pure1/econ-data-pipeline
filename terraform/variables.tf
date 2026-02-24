# terraform/variables.tf
# Variable declarations for all values referenced in main.tf.
# Override defaults with: terraform plan -var="gcp_project_id=my-project"
# Or use a tfvars file: terraform apply -var-file="prod.tfvars"

# ── AWS ───────────────────────────────────────────────────────────────────────

variable "aws_region" {
  description = "AWS region for S3 buckets and IAM resources."
  type        = string
  default     = "us-east-1"
}

# ── GCP ───────────────────────────────────────────────────────────────────────

variable "gcp_project_id" {
  description = "GCP project ID where BigQuery datasets and service accounts are created."
  type        = string
  # No default — must be provided explicitly to avoid creating resources in the wrong project.
}

variable "gcp_region" {
  description = "GCP region for regional resources (e.g. Cloud Run, Artifact Registry)."
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery dataset location. Use multi-region 'US' or 'EU' for production."
  type        = string
  default     = "US"
}

# ── Shared ────────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment. Controls resource naming and deletion protection."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "project_name" {
  description = "Base name used as a prefix for all provisioned resources."
  type        = string
  default     = "econ-pipeline"
}
