# --- Networking (default VPC) --------------------------------------------- #
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  # Only the default (public, IGW-routed) subnet per AZ — a Fargate task in a
  # private subnet with assignPublicIp can't actually reach S3/GHCR.
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

# Egress-only SG: the task reaches S3 + GHCR over the internet gateway.
resource "aws_security_group" "rollup" {
  name        = "rail-archiver-rollup"
  description = "Egress-only for the Fargate rollup task"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- ECS cluster (Fargate Spot) ------------------------------------------- #
resource "aws_ecs_cluster" "main" {
  name = "rail-archiver"
}

resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name       = aws_ecs_cluster.main.name
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE_SPOT"
    weight            = 1
  }
}

resource "aws_cloudwatch_log_group" "rollup" {
  name              = "/ecs/rail-archiver-rollup"
  retention_in_days = var.log_retention_days
}

# --- Task command --------------------------------------------------------- #
# Overlay rollup_source=s3 + the prod hot bucket onto the baked feeds.yaml (so
# there's no second config to keep in sync), then rollup + ship (hot parquet AND
# the cold DEEP_ARCHIVE tarball — cold_bucket is already prod in feeds.yaml).
# ROLLUP_DAY can be set per run (override) to target a specific past day;
# defaults to yesterday-UTC. This task also prunes the S3 landing at the end of
# its script (2026-09-03): the blind lifecycle rule that used to do it was
# deleting days that had never been shipped -- see landing.tf. Only THIS task
# prunes; none of the heavy-agency tasks (heavy_stages.tf) may, since prune_s3
# sweeps the whole bucket and they all run in the same 03:30Z slot.
#
# NOTE: this WAS split into three separate ECS tasks (rollup/gold/ship) chained
# by Step Functions, on the theory that a failed stage shouldn't silently
# re-run the ones before it. Reverted 2026-07-31: each Fargate task gets fresh
# ephemeral disk, and rollup/gold/ship all assume they share local disk within
# one container (curated_dir is a local Path — see archiver/rollup.py,
# gold.py, archiver/shipper.py). Splitting without shared storage (EFS, or
# teaching the Python classes to read/write curated data via S3) meant gold
# saw an empty curated/ and silently no-op'd every feed. Revisit the split
# once one of those is in place.
locals {
  # Agencies pulled out of the main rollup task so they get their own memory
  # ceiling instead of competing with 3 concurrent siblings for the main
  # task's rollup_memory.
  #
  # Expanded 2026-09-03 from just GO_AHEAD to every agency with a SIGKILL in the
  # preceding month. A log sweep of /ecs/rail-archiver-rollup found 42 exit -9
  # kills, and all of them belong to the seven agencies added here (25 in
  # snapshot.py, 17 in gold.py); the other ~197 agencies contributed none. The
  # main task therefore stops containing any known hog.
  #
  # Until 2026-09-05 these agencies ran their whole chain in ONE task
  # (rollup_heavy) under one memory ceiling, on the theory that a split by
  # pipeline STAGE wouldn't help since each agency here fails at the SAME step
  # every night (BKK/EDMONTON/LTC/VBB in snapshot, HOUSTON/CINCINNATI/SOFIA in
  # gold) -- but that reasoning conflated two different splits. Putting all
  # 204 golds in ONE task would indeed concentrate the hogs; isolating just
  # these three INTO THEIR OWN stage-scoped task does not, and decode/
  # cold-ship/hot-ship has never been the SIGKILL source for ANY of the eight
  # -- only gtfs.py (GO_AHEAD) and snapshot.py/gold.py (the rest) have. Paying
  # one 20 GiB ceiling for every stage of every heavy agency's run, every
  # night, to cover a failure mode that only ever hit one stage at a time was
  # the actual waste. See heavy_stages.tf for the per-stage replacement
  # (heavy_rollup/heavy_gtfs/heavy_snapshot/heavy_gold), each sized to the
  # failure it actually covers.
  #
  # CAVEAT on the four snapshot victims: until the 2026-09-03 archive-first
  # reorder, snapshot was step 1 and run_agency fail-fasts, so on the nights they
  # died their rollup/gtfs/gold never executed at all -- "rollup never OOMed" is
  # partly just "rollup never ran". Now that snapshot runs last (and, as of
  # heavy_stages.tf, in its own task), those four attempt every stage for the
  # first time, which is MORE memory in flight per stage, not less.
  heavy_agencies = [
    "GO_AHEAD",                            # SIGKILL in gtfs.py at 8 GiB even alone (2026-08-20)
    "BKK",                                 # snapshot.py, 13 GB/day of raw on bkk-trips alone
    "EDMONTON_TRANSIT_SYSTEM",             # snapshot.py
    "LONDON_TRANSIT_COMMISSION",           # snapshot.py, borderline -- died some nights, not others
    "VBB",                                 # snapshot.py, every night
    "METRO_HOUSTON",                       # gold.py, every night since 2026-08-10
    "CINCINNATI_METRO",                    # gold.py
    "URBAN_MOBILITY_CENTER_SOFIA_TRAFFIC", # gold.py
  ]

  # Which stages the MAIN task still owns, derived from the same variable that
  # enables the per-stage tasks (stages.tf) so the two can never drift. This is
  # deliberate: the GO_AHEAD outage came from exactly that drift -- an applied
  # --exclude-agency with its companion schedule left DISABLED meant the agency
  # was processed by NEITHER task for two weeks, silently. Tying both sides to
  # one flag makes the handover atomic in a single apply.
  #
  # cold-ship moves to the archive stage, which is a strict improvement on the
  # 2026-09-03 archive-first reorder: the raw tarball stops being downstream of
  # anything at all, rather than merely being first in a chain that could still
  # die before reaching it. gold moves to its own stage now that it reads
  # silver from the hot bucket (analysis/curated_fs.py, verified 2026-09-05)
  # instead of shared local disk -- phase 2 landed, so it drops out of
  # main_stages the same apply gold's own S3 read path went live. (Before this
  # fix, main_stages left "gold" in here too, so the state machine's own Gold
  # step -- stage_orchestration.tf -- ran the SAME agencies' gold a SECOND
  # time every night: once here reading local disk, once there reading S3.)
  main_stages = (
    var.stage_schedule_enabled
    ? "rollup"
    : "cold-ship rollup gtfs gold snapshot hot-ship"
  )

  # Landing cleanup. This replaced the expire-landing lifecycle rule on
  # 2026-09-03 (see landing.tf for why a blind time rule was destroying
  # unshipped data). prune_s3 confirms each day's cold tarball exists before
  # deleting that day, so it is safe to run even after a partial batch -- the
  # agencies that just failed simply get skipped, and their bins survive to be
  # re-shipped once the failure is fixed. `|| true` so a prune problem can never
  # mask agency_batch's exit code, which is what the alarms key off. Exactly one
  # task may prune (it sweeps the whole bucket); once the stage tasks are live
  # that task is `archive`, so this drops out here.
  main_prune = (
    var.stage_schedule_enabled
    ? ""
    : "python pipeline/prune_s3.py --config /tmp/fargate.yaml --keep-days ${var.landing_prune_keep_days} || true"
  )

  rollup_script = <<-EOT
    set -e
    DAY="$${ROLLUP_DAY:-$(date -u -d yesterday +%F)}"
    echo "rollup day: $DAY"
    # awsvpc puts every container in one netns, so the rollup reaches the
    # datadog-agent sidecar at 127.0.0.1:8125; env=prod matches the dashboard filter.
    python -c 'import os, yaml; c = yaml.safe_load(open("config/feeds.yaml")); c["writer"]["rollup_source"] = "s3"; c["s3"]["hot_bucket"] = os.environ["HOT_BUCKET"]; c["telemetry"]["enabled"] = True; c["telemetry"]["agent_host"] = "127.0.0.1"; c["telemetry"]["env"] = "prod"; yaml.safe_dump(c, open("/tmp/fargate.yaml", "w"))'
    # Task-level wall-clock duration, for the ECS runtime-cost dashboard
    # widgets. `trap ... EXIT` fires on both the success path and any set -e
    # early-exit, so a run that fails partway is still billed-cost-tracked
    # instead of silently dropped.
    START=$(date +%s)
    trap 'python pipeline/task_duration.py --config /tmp/fargate.yaml --seconds $(( $(date +%s) - START )) || true' EXIT

    # 2026-08-17: rollup/gtfs/gold/ship replaced with pipeline/agency_batch.py,
    # which runs each agency's slice of that same chain as its own disposable
    # `python pipeline/X.py` subprocess instead of one long-lived process per
    # step handling all ~186 agencies. Root cause of 3 consecutive nightly OOM
    # kills (days Aug 13-15, exitCode 137): pandas/pyarrow allocations inside a
    # long-lived process don't reliably get returned to the OS even after correct
    # Python-level GC -- proven by adding correct per-agency cache eviction to
    # gtfs.py's resolver_cache and observing zero change in when the OOM hit. An
    # OS process boundary is the only thing that reliably reclaims it. gold.py's
    # own _make_gtfs_resolver() had the same unbounded-cache shape (a second,
    # previously-unknown instance of the same bug) -- fixed for free by this
    # restructure with zero code changes to gold.py, since invoking it once per
    # agency instead of once per fleet naturally bounds it. --include-gtfs folds
    # gtfs.py into the same per-agency isolation for the same reason.
    #
    # 2026-08-18: --include-snapshot added after snapshot.py -- left standalone
    # above on the theory that it had no unbounded per-run state, unlike
    # gtfs.py/gold.py -- OOM-killed the very first scheduled run of this
    # restructured script (task 0b2143bc, day Aug 17), before agency_batch.py
    # ever got to start. Lesson: the identified caches were never the only
    # risk. ANY step handling the whole fleet sequentially in one long-lived
    # process is exposed to the same allocator-level issue, cache bug or not --
    # so nothing full-fleet is left in this script anymore.
    #
    # `set +e` / capture $? / `set -e` lets this capture agency_batch.py's exit
    # code without set -e killing the script mid-line (which would skip the
    # Datadog-drain sleep below) -- one agency's failure doesn't stop others
    # (see agency_batch.py's own docstring), but the container's own exit code
    # still needs to go nonzero on a bad day so ECS/CloudWatch alarms still fire.
    set +e
    python pipeline/agency_batch.py --config /tmp/fargate.yaml --day "$DAY" --stages ${local.main_stages} --exclude-agency ${join(" ", local.heavy_agencies)}
    AGENCY_STATUS=$?
    set -e
    if [ "$AGENCY_STATUS" -ne 0 ]; then
      echo "agency_batch: one or more agencies failed for $DAY -- see per-agency log lines above" >&2
    fi
    ${local.main_prune}
    # cert_check.py and s3_storage_metrics.py used to run here as auxiliary
    # `|| true` steps. Split out 2026-08-06 into their own scheduled ECS tasks
    # (cert_check.tf, aux_schedule.tf) — neither touches curated_dir or takes
    # --day, so unlike agency_batch they don't need this task's shared
    # local disk and don't need to ride along in this ~2.5h window.
    # Drain: DogStatsD is fire-and-forget UDP and the sidecar flushes on an
    # interval, so pause before the essential container exits (which SIGTERMs the
    # agent) to let the run's final ship.* metrics reach Datadog.
    sleep 15
    exit "$AGENCY_STATUS"
  EOT
}

# --- Task definition ------------------------------------------------------ #
resource "aws_ecs_task_definition" "rollup" {
  family                   = "rail-archiver-rollup"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.rollup_cpu
  memory                   = var.rollup_memory
  execution_role_arn       = aws_iam_role.rollup_execution.arn
  task_role_arn            = aws_iam_role.rollup_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64" # image is amd64 (multi-arch is a step-4 concern)
  }

  ephemeral_storage {
    size_in_gib = var.rollup_ephemeral_storage_gib
  }

  container_definitions = jsonencode([
    {
      name      = "rollup"
      image     = var.rollup_image
      essential = true
      command   = ["sh", "-c", local.rollup_script]
      environment = [
        { name = "HOT_BUCKET", value = var.hot_bucket },
        # botocore >=1.36 wraps the upload body in a non-seekable AwsChunkedWrapper
        # for its default request checksum; a retried PutObject then dies with
        # UnseekableStreamError. ship.py uses the same uploader, so disable the
        # default checksum here too (S3 does not require it).
        { name = "AWS_REQUEST_CHECKSUM_CALCULATION", value = "when_required" },
      ]
      # No agency API keys here: rollup/snapshot/gtfs/gold/ship all read
      # landing/curated data from S3, never a live feed URL, so they never
      # build an authenticated client (archiver/loader.py's build_feeds() is
      # metadata-only; build_agency_clients(), which DOES need keys, is only
      # ever called by the live poller). Injecting agency keys here used to be
      # a footgun, not a requirement: a new agency's key missing from
      # var.agency_secret_keys crashed this task for EVERY agency, not just
      # the new one (see the 2026-08-24 TFNSW outage).
      # Start the sidecar before the rollup so early metrics aren't dropped onto a
      # dead UDP socket (DogStatsD fails silently). START, not HEALTHY — no healthcheck.
      dependsOn = [
        { containerName = "datadog-agent", condition = "START" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.rollup.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "rollup"
        }
      }
    },
    {
      name      = "datadog-agent"
      image     = "gcr.io/datadoghq/agent:7" # same major as the box
      essential = false                      # task lifecycle follows `rollup`, not the agent
      memory    = 512                        # hard cap so the agent can't starve the rollup's working set
      environment = [
        { name = "DD_SITE", value = "datadoghq.com" },
        { name = "DD_DOGSTATSD_NON_LOCAL_TRAFFIC", value = "true" },
        { name = "DD_APM_ENABLED", value = "false" }, # we only use dogstatsd
        { name = "ECS_FARGATE", value = "true" },
      ]
      secrets = [
        { name = "DD_API_KEY", valueFrom = "${aws_secretsmanager_secret.env.arn}:DD_API_KEY::" }
      ]
      stopTimeout = 120 # Fargate max — lets SIGTERM trigger a final flush
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.rollup.name
          "awslogs-region"        = var.region
          "awslogs-stream-prefix" = "dd-agent"
        }
      }
    }

  ])
}

