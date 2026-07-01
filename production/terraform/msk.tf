# MSK (Managed Kafka) — the streaming ingestion backbone for 50K events/sec
# clickstream (Q3 part h). 3 brokers x m5.large, 48 partitions on the events
# topic, 7-day retention for replay. Kafka wire protocol keeps us portable.

resource "aws_msk_cluster" "clickstream" {
  cluster_name           = "semestabank-clickstream"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = var.msk_broker_count

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]
    storage_info { ebs_storage_info { volume_size = 1000 } }
  }

  encryption_info { encryption_in_transit { client_broker = "TLS" } }

  open_monitoring { prometheus { jmx_exporter { enabled_in_broker = true } } }
}

# Topic config is applied via the Kafka admin API / kafka_topic provider, e.g.:
#   topic "app_events"  partitions = 48  retention.ms = 604800000  (7 days)
# 48 partitions => up to 48 parallel Structured Streaming consumers; 25 MB/s
# peak (50K x 0.5KB) sits well within 3 x m5.large capacity.

variable "private_subnet_ids" { type = list(string)  default = [] }

resource "aws_security_group" "msk" {
  name        = "semestabank-msk"
  description = "MSK broker access from Databricks VPC only"
  vpc_id      = var.vpc_id
}

variable "vpc_id" { type = string  default = "" }
