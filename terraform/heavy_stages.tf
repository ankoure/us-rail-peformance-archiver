# --- Per-stage heavy-agency tasks (replaces rollup_heavy, 2026-09-05) ------ #
#
# local.heavy_agencies (rollup.tf) were isolated from the main fleet because
# they SIGKILL the main task's shared memory ceiling. Until today they ran
# their whole chain in ONE task (rollup_heavy) sized to the worst stage (20
# GiB, because GO_AHEAD's gtfs.py needs it) even though decode/cold-ship/
# hot-ship has never been the SIGKILL source for ANY of the eight -- only
# gtfs.py (GO_AHEAD) and snapshot.py/gold.py (the other seven) have. That
# meant paying one big ceiling for every stage of every heavy agency's run,
# every night, to cover a failure mode that only ever hit one stage at a time.
#
# This mirrors stages.tf's split for the non-heavy fleet, but split by BOTH
# stage and agency subset, since each heavy agency only fails at one specific
# stage (see rollup.tf's heavy_agencies comment):
#
#   heavy_rollup:   all 8 agencies. cold-ship + rollup + hot-ship -- the cheap
#                   stage, run at heavy_stage_workers concurrency instead of
#                   --workers 1 since nothing here has ever SIGKILLed.
#   heavy_gtfs:     GO_AHEAD only. Needs >8 GiB in gtfs.py alone (verified
#                   2026-08-20) -- the one heavy agency whose bottleneck isn't
#                   snapshot or gold.
#   heavy_snapshot: BKK/EDMONTON_TRANSIT_SYSTEM/LONDON_TRANSIT_COMMISSION/VBB.
#   heavy_gold:     METRO_HOUSTON/CINCINNATI_METRO/
#                   URBAN_MOBILITY_CENTER_SOFIA_TRAFFIC.
#
# heavy_gold reads silver from the hot bucket exactly like stage-gold (the
# --silver-dir s3://... path verified 2026-09-05) since it doesn't share local
# disk with heavy_rollup. heavy_gtfs and heavy_snapshot read nothing any other
# stage writes (same reasoning as the regular gtfs/snapshot stages), so they
# get a plain, un-sequenced daily schedule. heavy_rollup and heavy_gold have
# the one real ordering dependency (gold reads rollup's silver) and are
# sequenced by the state machine instead -- see stage_orchestration.tf's
# HeavyRollup -> HeavyGold branch.
#
# None of these four may run prune_s3 -- same reasoning as the main rollup
# task's comment: exactly one task sweeps the landing bucket, and that's the
# archive stage.

locals {
  heavy_stage_defs = {
    rollup = {
      cpu       = var.heavy_rollup_cpu
      memory    = var.heavy_rollup_memory
      stages    = "cold-ship rollup hot-ship"
      workers   = var.heavy_stage_workers
      agencies  = local.heavy_agencies
      silver    = ""
      scheduled = false # sequenced by the state machine (heavy_gold depends on it)
    }
    gtfs = {
      cpu       = var.heavy_gtfs_cpu
      memory    = var.heavy_gtfs_memory
      stages    = "gtfs"
      workers   = 1
      agencies  = ["GO_AHEAD"]
      silver    = ""
      scheduled = true # no ordering dependency -- plain cron, like the regular gtfs stage
    }
    snapshot = {
      cpu       = var.heavy_snapshot_cpu
      memory    = var.heavy_snapshot_memory
      stages    = "snapshot"
      workers   = 1
      agencies  = ["BKK", "EDMONTON_TRANSIT_SYSTEM", "LONDON_TRANSIT_COMMISSION", "VBB"]
      silver    = ""
      scheduled = true # backstopped by prune_s3's per-feed snapshot check, same as the regular snapshot stage's relationship to archive
    }
    gold = {
      cpu       = var.heavy_gold_cpu
      memory    = var.heavy_gold_memory
      stages    = "gold"
      workers   = 1
      agencies  = ["METRO_HOUSTON", "CINCINNATI_METRO", "URBAN_MOBILITY_CENTER_SOFIA_TRAFFIC"]
      silver    = "--silver-dir s3://${var.hot_bucket}"
      scheduled = false # sequenced by the state machine, after heavy_rollup
    }
  }

  # Same shape as stages.tf's stage_scripts, --agency-scoped instead of
  # --exclude-agency'd (see rollup_heavy_script's old docstring, now replaced).
  heavy_stage_scripts = {
    for name, def in local.heavy_stage_defs : name => <<-EOT
      set -e
      DAY="$${ROLLUP_DAY:-$(date -u -d yesterday +%F)}"
      echo "heavy stage ${name} day: $DAY, agencies: ${join(" ", def.agencies)}"
      python -c 'import os, yaml; c = yaml.safe_load(open("config/feeds.yaml")); c["writer"]["rollup_source"] = "s3"; c["s3"]["hot_bucket"] = os.environ["HOT_BUCKET"]; c["telemetry"]["enabled"] = True; c["telemetry"]["agent_host"] = "127.0.0.1"; c["telemetry"]["env"] = "prod"; yaml.safe_dump(c, open("/tmp/fargate.yaml", "w"))'
      START=$(date +%s)
      trap 'python pipeline/task_duration.py --config /tmp/fargate.yaml --metric pipeline.heavy_${name}.duration --seconds $(( $(date +%s) - START )) || true' EXIT

      set +e
      python pipeline/agency_batch.py --config /tmp/fargate.yaml --day "$DAY" --workers ${def.workers} --stages ${def.stages} ${def.silver} --agency ${join(" ", def.agencies)}
      AGENCY_STATUS=$?
      set -e
      if [ "$AGENCY_STATUS" -ne 0 ]; then
        echo "agency_batch (heavy ${name}): one or more agencies failed for $DAY -- see per-agency log lines above" >&2
      fi
      sleep 15
      exit "$AGENCY_STATUS"
    EOT
  }
}

resource "aws_cloudwatch_log_group" "heavy_stage" {
  for_each          = local.heavy_stage_defs
  name              = "/ecs/rail-archiver-heavy-${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_task_definition" "heavy_stage" {
  for_each                 = local.heavy_stage_defs
  family                   = "rail-archiver-heavy-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  # Reuses the rollup roles -- identical S3 + secrets access, same as stages.tf.
  execution_role_arn = aws_iam_role.rollup_execution.arn
  task_role_arn      = aws_iam_role.rollup_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  # No explicit ephemeral_storage block -- Fargate's unconfigured 20 GiB
  # default is generous for a handful of agencies (same reasoning the old
  # rollup_heavy task's comment gave).

  container_definitions = jsonencode([
    {
      name      = "heavy-${each.key}"
      image     = var.rollup_image
      essential = true
      command   = ["sh", "-c", local.heavy_stage_scripts[each.key]]
      environment = [
        { name = "HOT_BUCKET", value = var.hot_bucket },
        { name = "AWS_REQUEST_CHECKSUM_CALCULATION", value = "when_required" },
      ]
      dependsOn = [
        { containerName = "datadog-agent", condition = "START" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.heavy_stage[each.key].name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "heavy-${each.key}"
        }
      }
    },
    {
      name        = "datadog-agent"
      image       = "gcr.io/datadoghq/agent:7"
      essential   = false
      memory      = 512
      stopTimeout = 120
      environment = [
        { name = "DD_SITE", value = "datadoghq.com" },
        { name = "DD_DOGSTATSD_NON_LOCAL_TRAFFIC", value = "true" },
        { name = "DD_APM_ENABLED", value = "false" },
        { name = "ECS_FARGATE", value = "true" },
      ]
      secrets = [
        { name = "DD_API_KEY", valueFrom = "${aws_secretsmanager_secret.env.arn}:DD_API_KEY::" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.heavy_stage[each.key].name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "dd-agent"
        }
      }
    },
  ])
}

# gtfs and snapshot have no ordering dependency (see header comment), so they
# get a plain daily schedule via the same rollup_scheduler role the main
# rollup and regular stage-gtfs tasks use. rollup and gold are sequenced by
# the state machine instead (stage_orchestration.tf) -- this resource's
# for_each filters them out, same pattern as aws_scheduler_schedule.stage.
resource "aws_scheduler_schedule" "heavy_stage" {
  for_each = { for k, v in local.heavy_stage_defs : k => v if v.scheduled }
  name     = "rail-archiver-heavy-${each.key}-daily"
  state    = var.stage_schedule_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.stage_schedule_expression
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_ecs_cluster.main.arn
    role_arn = aws_iam_role.rollup_scheduler.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.heavy_stage[each.key].arn
      task_count          = 1
      tags                = { trigger = "scheduled" }
      # On-demand, not Spot -- same reasoning as every other task that
      # processes a day's data end-to-end and must complete.
      launch_type = "FARGATE"

      network_configuration {
        subnets          = data.aws_subnets.default.ids
        security_groups  = [aws_security_group.rollup.id]
        assign_public_ip = true
      }
    }
  }
}
