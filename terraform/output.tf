output "table_bucket_arn" {
  description = "ARN of the S3 Tables table bucket."
  value       = aws_s3tables_table_bucket.nyc_taxi.arn
}

output "glue_catalog_name" {
  description = "Name of the federated AWS Glue catalog for S3 Tables."
  value       = aws_glue_catalog.s3_tables.name
}
