# MWAA (Managed Airflow) + SNS alerting. MWAA runs the same DAG shape as the
# local Airflow (aws/mwaa_dag.py). SNS is the on-call alert channel referenced
# by both the MWAA DAG and the Step Functions state machine.

resource "aws_mwaa_environment" "orchestrator" {
  name              = "semestabank-orchestrator"
  airflow_version   = "2.10.1"
  environment_class = "mw1.small"
  dag_s3_path       = "dags/"
  source_bucket_arn = aws_s3_bucket.lake["artifacts"].arn
  execution_role_arn = aws_iam_role.mwaa_exec.arn

  network_configuration {
    security_group_ids = [aws_security_group.msk.id]
    subnet_ids         = var.private_subnet_ids
  }
}

resource "aws_sns_topic" "alerts" {
  name = "semestabank-data-eng-alerts"
}

resource "aws_sns_topic_subscription" "pagerduty" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "https"
  endpoint  = "https://events.pagerduty.com/integration/PLACEHOLDER/enqueue"
}

# Minimal IAM placeholders (real policies scoped to S3/EMR/Secrets/MSK).
resource "aws_iam_role" "mwaa_exec" {
  name               = "semestabank-mwaa-exec"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Effect = "Allow", Principal = { Service = "airflow-env.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}
