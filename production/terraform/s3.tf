# S3 — the lakehouse storage layer (Bronze/Silver/Gold as Delta) + hot/warm/cold
# lifecycle tiering (Q3 part h). Versioning + lock support OJK's 7-year audit.

resource "aws_s3_bucket" "lake" {
  for_each = var.medallion_buckets
  bucket   = each.value
}

resource "aws_s3_bucket_versioning" "lake" {
  for_each = aws_s3_bucket.lake
  bucket   = each.value.id
  versioning_configuration { status = "Enabled" }
}

# Hot (S3 Standard, 0-30d) -> Warm (Standard-IA, 30d-2y) -> Cold (Glacier IR
# then Deep Archive, 2-7y). Keeps the 7-year OJK retention cheap.
resource "aws_s3_bucket_lifecycle_configuration" "tiering" {
  for_each = { for k, v in aws_s3_bucket.lake : k => v if contains(["bronze", "silver", "gold"], k) }
  bucket   = each.value.id

  rule {
    id     = "medallion-tiering"
    status = "Enabled"
    transition { days = 30  storage_class = "STANDARD_IA" }
    transition { days = 730 storage_class = "GLACIER_IR" }
    transition { days = 1095 storage_class = "DEEP_ARCHIVE" }
    expiration { days = 2555 } # ~7 years, OJK retention boundary
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  for_each = aws_s3_bucket.lake
  bucket   = each.value.id
  rule { apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" } }
}
