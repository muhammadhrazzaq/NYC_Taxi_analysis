resource "aws_glue_catalog" "s3_tables" {
  name = var.catalog_name

  federated_catalog {
    connection_name = "aws:s3tables"
    identifier      = "arn:${data.aws_partition.current.partition}:s3tables:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:bucket/*"
  }

  create_database_default_permissions {
    permissions = ["ALL"]

    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }

  create_table_default_permissions {
    permissions = ["ALL"]

    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }

  allow_full_table_external_data_access = "True"

  depends_on = [aws_s3tables_table_bucket.nyc_taxi]
}
