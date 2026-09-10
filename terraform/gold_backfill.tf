# --- gold-backfill: reprocess gold.py's metrics marts for past days, ------ #
# --- WITHOUT re-running rollup.py ------------------------------------------ #
# Manual-only — no EventBridge schedule, unlike rollup/cert_check/
# s3_storage_metrics. Invoke by hand after fixing a gold.py bug and wanting to
# reprocess a range of already-shipped days:
#
#   aws ecs run-task --cluster rail-archiver --launch-type FARGATE_SPOT \
#     --task-definition rail-archiver-gold-backfill \
#     --network-configuration "awsvpcConfiguration={subnets=[<default-subnet-id>,...],securityGroups=[<rollup-sg-id>],assignPublicIp=ENABLED}" \
#     --overrides '{"containerOverrides":[{"name":"gold-backfill","environment":[
#       {"name":"FEEDS","value":"wmata-vehicles mbta-vehicles"},
#       {"name":"START_DAY","value":"2026-07-20"},
#       {"name":"END_DAY","value":"2026-08-01"}
#     ]}]}'
#
# See pipeline/gold_backfill.py's module docstring for why this pulls the
# already-shipped vehicles parquet back from the hot bucket instead of
# re-running rollup.py, and ships via Shipper.ship_one() directly instead of
# pipeline/ship.py's CLI.

resource "aws_cloudwatch_log_group" "gold_backfill" {
  name              = "/ecs/rail-archiver-gold-backfill"
  retention_in_days = var.log_retention_days
}

# Minimal task role: gold_backfill.py's ship_one(hot_only=True) never touches
# landing or cold, so this role only needs the hot bucket, unlike rollup_task.
resource "aws_iam_role" "gold_backfill_task" {
  name               = "rail-archiver-gold-backfill-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy" "gold_backfill_task" {
  name = "gold-backfill-hot-bucket"
  role = aws_iam_role.gold_backfill_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadWriteHot"
        Effect = "Allow"
        # GetObject: pull the already-shipped vehicles parquet back down.
        # HeadObject: authorize Uploader.exists()'s HeadObject call -- a
        # distinct IAM action from GetObject, not covered by it. PutObject:
        # ship the rebuilt metrics marts back.
        Action   = ["s3:GetObject", "s3:HeadObject", "s3:PutObject"]
        Resource = ["arn:aws:s3:::${var.hot_bucket}/*"]
      },
      {
        # Without bucket-level ListBucket, S3 can't tell "key doesn't exist"
        # from "you can't see it" and returns 403 instead of 404 for
        # HeadObject on a genuinely-missing key -- which Uploader.exists()
        # doesn't recognize as "missing" (it only special-cases 404/NoSuchKey),
        # so it re-raises. gold_backfill.py's exists() gate is exactly how it
        # detects a (feed, day) with no shipped vehicles parquet yet to skip
        # gracefully (see its module docstring: "gaps are expected"), so
        # without this the loop dies on the first ordinary gap instead.
        Sid      = "ListHotForExists"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = ["arn:aws:s3:::${var.hot_bucket}"]
      },
    ]
  })
}

locals {
  gold_backfill_script = <<-EOT
    set -e
    FEEDS="$${FEEDS:?set FEEDS to a space-separated list of feed names, e.g. \"wmata-vehicles mbta-vehicles\"}"
    START_DAY="$${START_DAY:?set START_DAY (YYYY-MM-DD)}"
    END_DAY="$${END_DAY:-$START_DAY}"

    # Same telemetry overlay the other split-out tasks use — see cert_check.tf's
    # comment for why agent_host must be 127.0.0.1 on Fargate. gold.py itself
    # emits no metrics; Shipper.ship_one's ship.hot.* does.
    python -c 'import yaml; c = yaml.safe_load(open("config/feeds.yaml")); c["telemetry"]["enabled"] = True; c["telemetry"]["agent_host"] = "127.0.0.1"; c["telemetry"]["env"] = "prod"; yaml.safe_dump(c, open("/tmp/fargate.yaml", "w"))'

    python pipeline/gold_backfill.py --config /tmp/fargate.yaml --feed $FEEDS --start-day "$START_DAY" --end-day "$END_DAY"

    # Drain: let ship_one's final ship.hot.* metrics reach the sidecar before SIGTERM.
    sleep 15
  EOT
}

resource "aws_ecs_task_definition" "gold_backfill" {
  family                   = "rail-archiver-gold-backfill"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  # Reuse rollup's cpu rather than guessing a smaller number: gold.py's
  # per-day compute (GTFS resolution, pandas OTP, segment speed) is the same
  # memory-hungry step that used to run inside the combined rollup task.
  # Memory used to reuse rollup_memory too, for the same reason (a real
  # 7.8 GiB measured peak) -- split onto its own gold_backfill_memory variable
  # on 2026-09-10 so rollup_memory's rightsizing (see its comment) doesn't
  # also cut this rare/manual job's margin. The original 1024/2048 here was an
  # unmeasured guess and SIGKILL'd (OOM) partway through gcrta-vehicles.
  cpu                = var.rollup_cpu
  memory             = var.gold_backfill_memory
  execution_role_arn = aws_iam_role.rollup_execution.arn
  task_role_arn      = aws_iam_role.gold_backfill_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64" # image is amd64, same as the rollup task
  }

  container_definitions = jsonencode([
    {
      name      = "gold-backfill"
      image     = var.rollup_image
      essential = true
      command   = ["sh", "-c", local.gold_backfill_script]
      dependsOn = [
        { containerName = "datadog-agent", condition = "START" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.gold_backfill.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "gold-backfill"
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
          "awslogs-group"         = aws_cloudwatch_log_group.gold_backfill.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "dd-agent"
        }
      }
    }
  ])
}
