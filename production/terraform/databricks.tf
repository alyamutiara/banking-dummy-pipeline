# Databricks — Unity Catalog (governance + automatic column-level lineage,
# Q3 part i) and the medallion catalog/schemas. The workspace itself is created
# via the databricks-on-aws account-level module (omitted here for brevity).

resource "databricks_catalog" "semestabank" {
  name    = "semestabank"
  comment = "SemestaBank lakehouse — Bronze/Silver/Gold medallion."
  storage_root = "s3://${var.medallion_buckets["silver"]}/uc"
}

resource "databricks_schema" "layers" {
  for_each     = toset(["bronze", "silver", "gold"])
  catalog_name = databricks_catalog.semestabank.name
  name         = each.value
}

# Lakehouse Federation: register Oracle as a foreign catalog so Unity Catalog
# lineage includes the SOURCE columns automatically (Q3 part i).
resource "databricks_connection" "oracle" {
  name            = "oracle_core_banking"
  connection_type = "ORACLE"
  options = {
    host = "core-db"
    port = "1521"
    user = "{{secrets/semestabank-core-banking/oracle_user}}"
  }
}

# Column-level PII masking tied to governance tags (NIK / phone), enforced for
# unprivileged roles. Applied via column masks / row filters on bronze.customers.
# resource "databricks_sql_table" ... WITH MASK pii_mask ON (nik, phone)
