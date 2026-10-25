variable "table_bucket_name" {
  description = "Name for the Amazon S3 Tables table bucket."
  type        = string
  default     = "nyc-taxi-analysis-tables"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.table_bucket_name))
    error_message = "The table bucket name must be 3-63 characters, using lowercase letters, numbers, and hyphens, and must start and end with a letter or number."
  }
}

variable "catalog_name" {
  description = "Name for the federated AWS Glue catalog for S3 Tables."
  type        = string
  default     = "s3tablescatalog"

  validation {
    condition     = can(regex("^[a-zA-Z0-9][a-zA-Z0-9_-]{1,61}[a-zA-Z0-9]$", var.catalog_name))
    error_message = "The catalog name must be 3-63 characters, using letters, numbers, underscores, and hyphens, and must start and end with a letter or number."
  }
}

variable "snowflake_org" { type = string }
variable "snowflake_account" { type = string }
variable "snowflake_user" { type = string }
variable "snowflake_private_key_path" { type = string }