# =====================================================================
# SemestaBank Data Platform — Terraform (ILLUSTRATIVE SKELETON)
# ---------------------------------------------------------------------
# ⚠️  This is a DESIGN STUB, not a validated/deployable module. It shows the
#     shape of the infrastructure-as-code for the Q3 architecture so a reviewer
#     can see "Terraform, not ClickOps". Resource arguments are abbreviated;
#     a real deployment needs networking, KMS, tagging, and state-backend wiring.
#     It has NOT been `terraform apply`-ed (no cloud account in this exercise).
# =====================================================================
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws        = { source = "hashicorp/aws", version = "~> 5.0" }
    databricks = { source = "databricks/databricks", version = "~> 1.40" }
  }
  # backend "s3" { bucket = "semestabank-tfstate" key = "platform/terraform.tfstate" region = "ap-southeast-1" dynamodb_table = "tf-locks" }
}

provider "aws" {
  region = var.region
  default_tags { tags = { project = "semestabank-data-platform", managed_by = "terraform" } }
}

provider "databricks" {
  host = var.databricks_workspace_url
}
