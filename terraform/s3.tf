resource "aws_s3tables_table_bucket" "nyc_taxi" {
  name = var.table_bucket_name
}

resource "aws_s3tables_namespace" "layers" {
  for_each = toset(["bronze", "silver", "gold"])

  namespace        = each.value
  table_bucket_arn = aws_s3tables_table_bucket.nyc_taxi.arn
}