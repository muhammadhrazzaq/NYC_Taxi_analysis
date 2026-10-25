locals {
  snowflake_role_name = "snowflake-s3tables-read"
  snowflake_role_arn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.snowflake_role_name}"
  region              = data.aws_region.current.region
}

# 1. Catalog integration (role ARN constructed, role created below)
resource "snowflake_execute" "catalog_integration" {
  execute = <<-SQL
    CREATE OR REPLACE CATALOG INTEGRATION S3TABLES_NYC_TAXI
      CATALOG_SOURCE = ICEBERG_REST
      TABLE_FORMAT   = ICEBERG
      REST_CONFIG = (
        CATALOG_URI            = 'https://s3tables.${local.region}.amazonaws.com/iceberg'
        CATALOG_API_TYPE       = AWS_S3TABLES
        CATALOG_NAME           = '${aws_s3tables_table_bucket.nyc_taxi.arn}'
        ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
      )
      REST_AUTHENTICATION = (
        TYPE                 = SIGV4
        SIGV4_IAM_ROLE       = '${local.snowflake_role_arn}'
        SIGV4_SIGNING_REGION = '${local.region}'
      )
      ENABLED = TRUE
  SQL
  revert  = "DROP CATALOG INTEGRATION IF EXISTS S3TABLES_NYC_TAXI"
  query   = "DESC CATALOG INTEGRATION S3TABLES_NYC_TAXI"
}

locals {
  sf_props = {
    for row in snowflake_execute.catalog_integration.query_results :
    row.property => row.property_value
  }
}

# 2. IAM role trusted only by this Snowflake integration
resource "aws_iam_role" "snowflake" {
  name = local.snowflake_role_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { AWS = local.sf_props["API_AWS_IAM_USER_ARN"] }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "sts:ExternalId" = local.sf_props["API_AWS_EXTERNAL_ID"] } }
    }]
  })
}

data "aws_iam_policy_document" "snowflake_read" {
  statement {
    actions = [
      "s3tables:GetTableBucket", "s3tables:ListNamespaces", "s3tables:GetNamespace",
      "s3tables:ListTables", "s3tables:GetTable",
      "s3tables:GetTableMetadataLocation", "s3tables:GetTableData",
    ]
    resources = [
      aws_s3tables_table_bucket.nyc_taxi.arn,
      "${aws_s3tables_table_bucket.nyc_taxi.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "snowflake" {
  role   = aws_iam_role.snowflake.id
  policy = data.aws_iam_policy_document.snowflake_read.json
}

resource "time_sleep" "wait_for_iam_propagation" {
  create_duration = "30s"

  depends_on = [
    aws_iam_role.snowflake,
    aws_iam_role_policy.snowflake
  ]
}

# 3. Catalog-linked database (only after the role works)
resource "snowflake_execute" "linked_db" {
  execute = <<-SQL
    CREATE OR REPLACE DATABASE NYC_TAXI
      LINKED_CATALOG = (
        CATALOG            = 'S3TABLES_NYC_TAXI'
        ALLOWED_NAMESPACES = ('gold')
      )
  SQL
  revert  = "DROP DATABASE IF EXISTS NYC_TAXI"
  query   = "SHOW DATABASES LIKE 'NYC_TAXI'"

  depends_on = [aws_iam_role_policy.snowflake]
}