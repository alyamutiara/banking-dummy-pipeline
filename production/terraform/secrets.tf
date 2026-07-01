# Secrets Manager — Oracle core-banking credentials. This is the production
# answer to Q1 problem #1 (hardcoded creds): nothing in code, everything here,
# read at runtime by the Spark jobs via instance-profile IAM, and surfaced to
# Databricks through a secret scope backed by this store.

resource "aws_secretsmanager_secret" "oracle" {
  name        = "semestabank/core-banking/oracle"
  description = "Oracle ETL read-only user for the account-snapshot pipeline"
  kms_key_id  = aws_kms_key.secrets.id
}

# Value is set out-of-band (CI/CD or manual rotation) — never in Terraform code.
# resource "aws_secretsmanager_secret_version" "oracle" { ... } # intentionally omitted

resource "aws_kms_key" "secrets" {
  description             = "KMS key for SemestaBank secrets + S3 SSE"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

# Databricks secret scope backed by the AWS secret above.
resource "databricks_secret_scope" "core_banking" {
  name                     = "semestabank-core-banking"
  backend_type             = "DATABRICKS" # use AWS-backed scope where available
}
