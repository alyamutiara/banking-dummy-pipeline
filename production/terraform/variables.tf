# Input variables for the SemestaBank platform skeleton.
variable "region" {
  description = "AWS region — Singapore for ~12ms latency to Jakarta + Databricks availability."
  type        = string
  default     = "ap-southeast-1"
}

variable "account_id" {
  type    = string
  default = "000000000000"
}

variable "databricks_workspace_url" {
  type    = string
  default = "https://semestabank.cloud.databricks.com"
}

variable "medallion_buckets" {
  description = "Bronze/Silver/Gold lakehouse buckets (Delta lives here)."
  type        = map(string)
  default = {
    bronze    = "semestabank-bronze"
    silver    = "semestabank-silver"
    gold      = "semestabank-gold"
    artifacts = "semestabank-artifacts"
    logs      = "semestabank-logs"
  }
}

variable "msk_broker_count" {
  description = "MSK brokers — 3 x m5.large handles 50K events/sec at peak with headroom."
  type        = number
  default     = 3
}
