# --- historic-511-otp: parse landed 511 historic archives into OTP marts -- #
# Manual-only — no EventBridge schedule, same as gold_backfill.tf. Invoke by
# hand for the pilot backfill (a handful of already-landed months) via:
#
#   aws ecs run-task --cluster rail-archiver --launch-type FARGATE \
#     --task-definition rail-archiver-historic-511-otp \
#     --network-configuration "awsvpcConfiguration={subnets=[<default-subnet-id>,...],securityGroups=[<rollup-sg-id>],assignPublicIp=ENABLED}" \
#     --overrides '{"containerOverrides":[{"name":"historic-511-otp","environment":[
#       {"name":"MONTHS","value":"2026-02 2026-03 2026-04 2026-05 2026-06 2026-07"}
#     ]}]}'
#
# See pipeline/historic_511_otp.py's module docstring for why this needs no
# GTFS-join step of its own (511 already resolved the schedule match in
# stop_observations.txt) and ships via Shipper.ship_one(hot_only=True) into
# the SAME feed-partitioned mart paths the live pipeline uses.

resource "aws_cloudwatch_log_group" "historic_511_otp" {
  name              = "/ecs/rail-archiver-historic-511-otp"
  retention_in_days = var.log_retention_days
}

# Minimal task role: reads the already-landed raw zip and ships marts, both
# hot-bucket only (hot_only=True never touches landing or cold). Same shape
# as gold_backfill_task's policy in gold_backfill.tf.
resource "aws_iam_role" "historic_511_otp_task" {
  name               = "rail-archiver-historic-511-otp-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy" "historic_511_otp_task" {
  name = "historic-511-otp-hot-bucket"
  role = aws_iam_role.historic_511_otp_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadWriteHot"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = ["arn:aws:s3:::${var.hot_bucket}/*"]
      },
      {
        # Without bucket-level ListBucket, a HeadObject on a missing key
        # 403s instead of 404s (see gold_backfill.tf's ListHotForExists
        # comment) — Uploader.exists()'s idempotency gate depends on the 404.
        Sid      = "ListHotForExists"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = ["arn:aws:s3:::${var.hot_bucket}"]
      },
    ]
  })
}

locals {
  historic_511_otp_script = <<-EOT
    set -e
    MONTHS="$${MONTHS:?set MONTHS to a space-separated list of already-landed months, e.g. \"2026-06 2026-07\"}"
    python -c 'import yaml; c = yaml.safe_load(open("config/feeds.yaml")); c["telemetry"]["enabled"] = True; c["telemetry"]["agent_host"] = "127.0.0.1"; c["telemetry"]["env"] = "prod"; yaml.safe_dump(c, open("/tmp/fargate.yaml", "w"))'
    for MONTH in $MONTHS; do
      python pipeline/historic_511_otp.py --config /tmp/fargate.yaml --month "$MONTH"
    done
    # Drain: let the run's final ship.hot.* metrics reach the sidecar before SIGTERM.
    sleep 15
  EOT
}

resource "aws_ecs_task_definition" "historic_511_otp" {
  family                   = "rail-archiver-historic-511-otp"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  # Pandas over ~10M rows/month (SF alone) — same memory-hungry shape
  # gold_backfill_memory started from; historic_511_otp_memory carries the
  # same 16 GiB default (see its comment in variables.tf) rather than
  # guessing a smaller number, since this task hasn't run against full data
  # yet to measure against.
  cpu                = var.rollup_cpu
  memory             = var.historic_511_otp_memory
  execution_role_arn = aws_iam_role.rollup_execution.arn
  task_role_arn      = aws_iam_role.historic_511_otp_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64" # image is amd64, same as the rollup task
  }

  container_definitions = jsonencode([
    {
      name      = "historic-511-otp"
      image     = var.rollup_image
      essential = true
      command   = ["sh", "-c", local.historic_511_otp_script]
      dependsOn = [
        { containerName = "datadog-agent", condition = "START" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.historic_511_otp.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "historic-511-otp"
        }
      }
    },
    {
      name      = "datadog-agent"
      image     = "gcr.io/datadoghq/agent:7"
      essential = false
      memory    = 512
      environment = [
        { name = "DD_SITE", value = "datadoghq.com" },
        { name = "DD_DOGSTATSD_NON_LOCAL_TRAFFIC", value = "true" },
        { name = "DD_APM_ENABLED", value = "false" },
        { name = "ECS_FARGATE", value = "true" },
      ]
      secrets = [
        { name = "DD_API_KEY", valueFrom = "${aws_secretsmanager_secret.env.arn}:DD_API_KEY::" }
      ]
      stopTimeout = 120
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.historic_511_otp.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "dd-agent"
        }
      }
    }
  ])
}
