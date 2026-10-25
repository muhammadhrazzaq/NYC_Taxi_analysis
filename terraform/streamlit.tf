locals {
  app_dir   = "${path.module}/../apps/dashboard"
  app_files = ["streamlit_app.py", "snowflake.yml","pyproject.toml"]
  app_hash  = md5(join("", [for f in local.app_files : filemd5("${local.app_dir}/${f}")]))
  snow      = "/Users/mo/.local/bin/snow"   # your `which snow` path
}

resource "terraform_data" "upload_app" {
  triggers_replace = [local.app_hash]

  provisioner "local-exec" {
    command = join(" && ", [
      for f in local.app_files :
      "${local.snow} stage copy ${local.app_dir}/${f} @APPS.DASHBOARDS.STREAMLIT_STAGE/nyc_taxi --overwrite -c terraform"
    ])
  }

  depends_on = [snowflake_execute.stage]
}

resource "snowflake_warehouse" "dash" {
  name           = "DASH_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = "true"
}

resource "snowflake_database" "apps" {
  name = "APPS"
}

resource "snowflake_schema" "dashboards" {
  database = snowflake_database.apps.name
  name     = "DASHBOARDS"
}

resource "snowflake_execute" "stage" {
  execute = "CREATE STAGE IF NOT EXISTS APPS.DASHBOARDS.STREAMLIT_STAGE"
  revert  = "DROP STAGE IF EXISTS APPS.DASHBOARDS.STREAMLIT_STAGE"
  depends_on = [snowflake_schema.dashboards]
}


# (Re)create the app; the hash in the comment forces a redeploy when the code changes
resource "snowflake_execute" "streamlit" {
  execute = <<-SQL
    CREATE OR REPLACE STREAMLIT APPS.DASHBOARDS.NYC_TAXI_DASHBOARD
      FROM '@APPS.DASHBOARDS.STREAMLIT_STAGE/nyc_taxi'
      MAIN_FILE = 'streamlit_app.py'
      RUNTIME_NAME = 'SYSTEM$ST_CONTAINER_RUNTIME_PY3_11'
      COMPUTE_POOL = SYSTEM_COMPUTE_POOL_CPU
      QUERY_WAREHOUSE = ${snowflake_warehouse.dash.name}
      TITLE = 'NYC Taxi Lakehouse'
      COMMENT = 'code md5 ${local.app_hash}'
  SQL
  revert = "DROP STREAMLIT IF EXISTS APPS.DASHBOARDS.NYC_TAXI_DASHBOARD"
  query  = "SHOW STREAMLITS LIKE 'NYC_TAXI_DASHBOARD' IN SCHEMA APPS.DASHBOARDS"

  depends_on = [terraform_data.upload_app, snowflake_execute.linked_db]
}


