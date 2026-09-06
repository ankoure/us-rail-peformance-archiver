# --- Per-stage rollup tasks (phase 1 of the stage split) ------------------- #
#
# The nightly batch used to be ONE task at rollup_cpu/rollup_memory running every
# stage, which meant every stage was provisioned at the max of all of them:
# rollup is CPU-bound (Rust decode), gtfs/gold/snapshot are memory-bound, and the
# ships are I/O-bound. Sizing them together made headroom untargetable -- giving
# gold more memory meant giving rollup the same.
#
# These four stages read nothing that any OTHER STAGE writes (gtfs pulls static
# GTFS off the network, snapshot reads landing, cold-ship reads landing), so they
# split out with no shared-storage problem at all -- unlike gold, which reads
# rollup's silver parquet. That is what makes this safe where the 2026-07-31
# Step Functions split was not: see the NOTE in rollup.tf, where gold saw an
# empty curated/ and silently no-op'd every feed.
#
# snapshot and archive DO have a dependency on EACH OTHER, though: archive's
# prune_s3 deletes a landing day-partition once shipped, and must not run
# before snapshot has had its chance to read that day's raw payloads. That
# ordering is handled by the state machine in stage_orchestration.tf, same as
# gold's dependency on rollup -- see that file's header comment. So of the four,
# only gtfs is scheduled by a plain, un-ordered cron below.
#
# Each excludes only the agencies actually flagged for ITS OWN failure mode
# (heavy_stages.tf's local.heavy_stage_defs[name].agencies), not the full
# local.heavy_agencies list -- an agency isolated for gold.py (say,
# METRO_HOUSTON) has never been a gtfs.py or snapshot.py problem, so it runs
# gtfs/snapshot right here in the regular pool. Wiring these to
# heavy_stage_defs directly (rather than copy-pasting the same agency names
# twice) is what makes the two sides unable to drift apart, same principle as
# rollup.tf's main_stages being derived from var.stage_schedule_enabled.
#
# Regression fixed 2026-09-06, caught before it cost a single night: the
# first version of this split excluded the FULL heavy_agencies list from all
# four stages uniformly, then re-included each agency only in its own narrow
# heavy_stages.tf subset -- so e.g. GO_AHEAD (only ever flagged for gtfs.py)
# was excluded from stage-gold AND absent from heavy_gold's agency list,
# meaning its gold mart would never have been built by EITHER task, silently,
# every night. Same failure class as the GO_AHEAD outage described in
# rollup.tf's heavy_agencies comment -- excluded from one task without being
# re-included in the other -- just introduced by this split instead of a
# schedule left disabled.
#
# archive is the one exception: it still excludes the FULL heavy_agencies
# list, because cold-ship for all eight is handled by heavy_rollup, not by
# any of the narrower heavy_stages.tf subsets.
#
# Sizes below are deliberately generous first guesses, not measured values. Watch
# pipeline.<stage>.duration and the task memory graphs for a week before cutting
# any of them (same discipline as rollup_memory's history).

locals {
  stage_defs = {
    gtfs = {
      cpu       = var.stage_gtfs_cpu
      memory    = var.stage_gtfs_memory
      stages    = "gtfs"
      workers   = var.stage_workers
      post      = ""
      silver    = ""
      exclude   = local.heavy_stage_defs["gtfs"].agencies
      scheduled = true
    }
    # Sequenced by the state machine after nothing (it's the first stage in its
    # branch) but BEFORE archive, below -- see stage_orchestration.tf.
    snapshot = {
      cpu       = var.stage_snapshot_cpu
      memory    = var.stage_snapshot_memory
      stages    = "snapshot"
      workers   = var.stage_workers
      post      = ""
      silver    = ""
      exclude   = local.heavy_stage_defs["snapshot"].agencies
      scheduled = false
    }
    # cold-ship + prune. Splitting the archive out means the raw DEEP_ARCHIVE
    # tarball is no longer downstream of ANY other stage -- stronger than the
    # 2026-09-03 archive-first reorder, which only moved it to the front of a
    # chain that could still die before reaching it.
    #
    # prune_s3 runs here and ONLY here: it sweeps the whole landing bucket, so a
    # second task doing it concurrently would be pure duplicated listing. It is
    # safe to run while other stages are still working -- it deletes only days
    # whose cold tarball AND (for an alerts-capable feed) snapshot object are
    # already confirmed in S3 (see prune_s3's docstring), and keep-days holds
    # back the recent days everything else is actually reading.
    #
    # Sequenced by the state machine after snapshot, not by its own cron --
    # see stage_orchestration.tf for why archive can't just run on a plain
    # schedule alongside it.
    archive = {
      cpu       = var.stage_archive_cpu
      memory    = var.stage_archive_memory
      stages    = "cold-ship"
      workers   = var.stage_workers
      silver    = ""
      exclude   = local.heavy_agencies
      scheduled = false
      post      = "python pipeline/prune_s3.py --config /tmp/fargate.yaml --keep-days ${var.landing_prune_keep_days} || true"
    }
    # Phase 2. Unlike gtfs, gold READS another stage's output, so it must run
    # after rollup -- that ordering is the whole reason the state machine in
    # stage_orchestration.tf exists, and why this stage's schedule is driven
    # by the state machine rather than its own cron.
    #
    # It reads silver from the hot bucket (var.hot_bucket) rather than local
    # disk. That is not a new layout: Shipper._hot_key is the curated-relative
    # path, so the hot bucket already IS the curated tree, and pyarrow reads it
    # with row-group streaming (analysis/curated_fs.py). Marts are still written
    # to local ephemeral disk and uploaded by the implied hot-ship.
    gold = {
      cpu     = var.stage_gold_cpu
      memory  = var.stage_gold_memory
      stages  = "gold"
      workers = var.stage_workers
      post    = ""
      silver  = "--silver-dir s3://${var.hot_bucket}"
      exclude = local.heavy_stage_defs["gold"].agencies
      # Sequenced by the state machine after rollup, not by its own cron.
      scheduled = false
    }
  }

  # Same shell shape as local.rollup_script -- config overlay, duration trap,
  # set +e around agency_batch so one agency's failure still surfaces in the
  # task's exit code without skipping the Datadog drain.
  stage_scripts = {
    for name, def in local.stage_defs : name => <<-EOT
      set -e
      DAY="$${ROLLUP_DAY:-$(date -u -d yesterday +%F)}"
      echo "stage ${name} day: $DAY"
      python -c 'import os, yaml; c = yaml.safe_load(open("config/feeds.yaml")); c["writer"]["rollup_source"] = "s3"; c["s3"]["hot_bucket"] = os.environ["HOT_BUCKET"]; c["telemetry"]["enabled"] = True; c["telemetry"]["agent_host"] = "127.0.0.1"; c["telemetry"]["env"] = "prod"; yaml.safe_dump(c, open("/tmp/fargate.yaml", "w"))'
      START=$(date +%s)
      trap 'python pipeline/task_duration.py --config /tmp/fargate.yaml --metric pipeline.stage_${name}.duration --seconds $(( $(date +%s) - START )) || true' EXIT

      set +e
      python pipeline/agency_batch.py --config /tmp/fargate.yaml --day "$DAY" --workers ${def.workers} --stages ${def.stages} ${def.silver} --exclude-agency ${join(" ", def.exclude)}
      AGENCY_STATUS=$?
      set -e
      if [ "$AGENCY_STATUS" -ne 0 ]; then
        echo "agency_batch (${name}): one or more agencies failed for $DAY -- see per-agency log lines above" >&2
      fi
      ${def.post}
      sleep 15
      exit "$AGENCY_STATUS"
    EOT
  }
}

resource "aws_cloudwatch_log_group" "stage" {
  for_each          = local.stage_defs
  name              = "/ecs/rail-archiver-stage-${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_task_definition" "stage" {
  for_each                 = local.stage_defs
  family                   = "rail-archiver-stage-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  # Reuses the rollup roles: identical S3 + secrets access, and the landing
  # DeleteObject grant the archive stage's prune needs is already on them.
  execution_role_arn = aws_iam_role.rollup_execution.arn
  task_role_arn      = aws_iam_role.rollup_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([
    {
      name      = "stage-${each.key}"
      image     = var.rollup_image
      essential = true
      command   = ["sh", "-c", local.stage_scripts[each.key]]
      environment = [
        { name = "HOT_BUCKET", value = var.hot_bucket },
        { name = "AWS_REQUEST_CHECKSUM_CALCULATION", value = "when_required" },
      ]
      secrets = [
        { name = "MDB_REFRESH_TOKEN", valueFrom = "${aws_secretsmanager_secret.env.arn}:MDB_REFRESH_TOKEN::" }
      ]
      dependsOn = [
        { containerName = "datadog-agent", condition = "START" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.stage[each.key].name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "stage-${each.key}"
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
          "awslogs-group"         = aws_cloudwatch_log_group.stage[each.key].name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "dd-agent"
        }
      }
    },
  ])
}

# gtfs is the only stage left with no ordering dependency on anything else, so
# it's the only one still driven by a plain, un-sequenced schedule here.
# snapshot/archive/gold all run via the state machine in
# stage_orchestration.tf instead -- this resource's `for_each` filters them
# out (`scheduled = false`) so they don't ALSO get a redundant cron trigger.
resource "aws_scheduler_schedule" "stage" {
  for_each = { for k, v in local.stage_defs : k => v if v.scheduled }
  name     = "rail-archiver-stage-${each.key}-daily"
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
      task_definition_arn = aws_ecs_task_definition.stage[each.key].arn
      task_count          = 1
      tags                = { trigger = "scheduled" }
      # On-demand FARGATE, not Spot: same reasoning as the main rollup and the
      # heavy_stages.tf tasks -- these process a day's data and must complete.
      # The archive stage especially: a Spot reclaim mid-run means landing
      # days that never reach cold.
      launch_type = "FARGATE"

      network_configuration {
        subnets          = data.aws_subnets.default.ids
        security_groups  = [aws_security_group.rollup.id]
        assign_public_ip = true
      }
    }
  }
}
