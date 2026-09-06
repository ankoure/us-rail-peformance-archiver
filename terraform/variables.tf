variable "region" {
  type    = string
  default = "us-east-1"
}

variable "eu_region" {
  type        = string
  default     = "eu-central-1"
  description = "AWS region for the EU/Frankfurt poller box."
}

variable "au_region" {
  type        = string
  default     = "ap-southeast-2"
  description = "AWS region for the AU/Sydney poller box."
}

variable "aws_profile" {
  type        = string
  default     = null
  description = "AWS named profile for the provider to use (matches the aws CLI). Leave null to use the default credential chain / env vars."
}

variable "landing_bucket" {
  type        = string
  default     = "rail-performance-archiver-landing"
  description = "Prod S3 landing bucket the pollers dual-write to and the rollup reads."
}

variable "landing_prune_keep_days" {
  type = number
  # Replaced landing_retention_days (a blind 7-day lifecycle expiry) on
  # 2026-09-03. That variable's own comment claimed "prune_s3.py now handles
  # proactive cleanup, so the lifecycle is just a backstop" -- but prune_s3.py
  # was never actually scheduled anywhere, so the "backstop" was doing all the
  # deleting, including of days that had never been archived. See landing.tf.
  default     = 3
  description = <<-EOT
    Days of landing to retain when the nightly rollup task runs prune_s3.py.
    The rollup only needs yesterday; the rest is a buffer for re-rolls. Unlike
    the lifecycle rule this replaced, prune_s3 deletes a day only once its cold
    tarball is confirmed in S3 -- unshipped days are skipped, never deleted, so
    lowering this can't cost data.
  EOT
}

variable "instance_role_name" {
  type        = string
  description = <<-EOT
    Name of the EC2 instance role attached to the poller box (its instance
    profile). Terraform attaches a landing-bucket write policy to it WITHOUT
    managing the role itself. Find it with:
      aws iam list-instance-profiles --query 'InstanceProfiles[].Roles[].RoleName'
    (deploy/README.md references the profile "rail-archiver-instance").
  EOT
}

# --- 3b: Fargate rollup shadow task --------------------------------------- #

variable "rollup_image" {
  type        = string
  default     = "ghcr.io/ankoure/us-rail-performance-archiver:latest"
  description = "Container image for the rollup task (must be PUBLIC on GHCR for Fargate to pull without creds)."
}

variable "hot_scratch_bucket" {
  type        = string
  default     = "rail-performance-archiver-hot-scratch"
  description = "Scratch bucket the SHADOW rollup wrote parquet to (kept for re-shadowing; the prod task now writes hot_bucket)."
}

# Prod curated buckets — created out-of-band (not Terraform-managed), referenced
# by name. These match config/feeds.yaml s3.hot_bucket / s3.cold_bucket so the
# Fargate rollup ships to the same place the on-box batch did.
variable "hot_bucket" {
  type        = string
  default     = "rail-performance-archiver-hot"
  description = "Prod hot bucket the rollup writes curated parquet to (must match feeds.yaml s3.hot_bucket)."
}

variable "cold_bucket" {
  type        = string
  default     = "rail-performance-archiver-cold"
  description = "Prod cold bucket the rollup ships the DEEP_ARCHIVE tarball to (must match feeds.yaml s3.cold_bucket)."
}

# Re-enabled 2026-06-14: Shipper is now S3-aware (ships hot+cold from the S3
# landing, verified on 6/11+6/12) and the schedule runs on-demand FARGATE (not
# Spot). The end-to-end Fargate rollup+ship pipeline works, so the daily job is live.
variable "rollup_schedule_enabled" {
  type        = bool
  default     = true
  description = "Whether the daily EventBridge schedule that runs the rollup is ENABLED."
}

variable "rollup_schedule_expression" {
  type        = string
  default     = "cron(30 3 * * ? *)"
  description = "EventBridge Scheduler expression (UTC) for the daily rollup — ~03:30Z per the design doc."
}

variable "hot_scratch_retention_days" {
  type    = number
  default = 14
}

variable "rollup_cpu" {
  type        = string
  default     = "4096" # 4 vCPU
  description = "Fargate task CPU units for the rollup stage. Design targets up to 8192 (8 vCPU); start smaller and measure (the tail is a few big serial feeds, so more cores may not help)."
}

variable "rollup_memory" {
  type = string
  # 10 GiB (was dropped to 8 GiB when this reverted to one combined task,
  # against a pre-dedup measured peak of 4.59 GiB — see git history). Back to
  # 10 GiB: the 2026-08-03 rollup run hit 7.8 GiB (90% of the 8 GiB ceiling)
  # ~3.5 min in and triggered a BrokenProcessPool OOM kill across 4 concurrent
  # ROLLUP_WORKERS, the first run after Deduper started holding a Python-level
  # _seen set of dedup-key tuples per worker on top of the Arrow batch memory.
  #
  # 2026-08-16: bumped 10 -> 20 GiB as a stopgap. 3 consecutive nightly runs
  # (days Aug 13-15) hit exitCode 137 / "OutOfMemoryError: container killed
  # due to memory usage" -- a task-level OOM, not a single-worker crash -- and
  # none of them reached the ship step, so nothing shipped to hot for 3 days.
  # The first of the 3 died on the *old* 255-feed config, before the Aug 14
  # global-feed merge, so this predates and is likely broader than the feed
  # count increase. Root cause still open -- NOT the dedup set above, which
  # was removed in 0dfa987 the same day as the incident that comment
  # describes (2026-08-03), so it hasn't existed for the 13 days this bug's
  # been live. This bump just buys headroom while the real cause is found.
  #
  # 2026-08-20: dropped 20 -> 16 GiB after splitting local.heavy_agencies
  # (currently just GO_AHEAD, exit -9/SIGKILL on the 2026-08-19 run) out to
  # their own rollup_heavy task with its own memory ceiling -- see rollup.tf.
  # This is a first conservative step, not a measured value: the remaining
  # ~185 agencies still run concurrently (rollup_cpu workers) and share this
  # ceiling, so watch a few post-split runs before cutting further.
  default     = "16384"
  description = "Fargate task memory (MiB) for the rollup stage (excludes local.heavy_agencies, see rollup_heavy_memory)."
}

# --- Heavy-agency stages (heavy_stages.tf) --------------------------------- #
# Replaced the single rollup_heavy task on 2026-09-05. That task ran the WHOLE
# chain (cold-ship/rollup/gtfs/gold/snapshot/hot-ship) for all of
# local.heavy_agencies under ONE memory ceiling (20 GiB), even though the
# actual SIGKILLs only ever came from gtfs.py (GO_AHEAD) or snapshot.py/gold.py
# for the other seven -- decode + cold-ship + hot-ship has never OOMed for any
# of them. Paying 20 GiB for the whole run to cover a failure mode that only
# ever hit one stage was the thing being fixed: splitting by STAGE (like
# stages.tf already does for the non-heavy fleet) means each ceiling is sized
# to the failure it actually covers, not the worst case of all of them at once.
#
# heavy_rollup runs all 8 agencies (cheap, unlike the other three below).
# heavy_gtfs is GO_AHEAD only. heavy_snapshot is
# BKK/EDMONTON_TRANSIT_SYSTEM/LONDON_TRANSIT_COMMISSION/VBB. heavy_gold is
# METRO_HOUSTON/CINCINNATI_METRO/URBAN_MOBILITY_CENTER_SOFIA_TRAFFIC -- see
# heavy_stages.tf's local.heavy_stage_defs for the exact split.

variable "heavy_rollup_cpu" {
  type        = string
  default     = "2048" # 2 vCPU
  description = "Fargate CPU units for the heavy-agency rollup stage (decode + cold-ship + hot-ship for all 8 heavy agencies)."
}

variable "heavy_rollup_memory" {
  type        = string
  default     = "8192" # 8 GiB
  description = <<-EOT
    Fargate memory (MiB) for the heavy-agency rollup stage. Decode/cold-ship/
    hot-ship has never been the SIGKILL source for any heavy agency (only
    gtfs.py and snapshot.py/gold.py have) -- this is a generous first guess for
    8 agencies run at heavy_stage_workers concurrency, not the 20 GiB the old
    combined rollup_heavy task carried for every stage regardless of need.
  EOT
}

variable "heavy_gtfs_cpu" {
  type        = string
  default     = "4096" # 4 vCPU -- paired tier for 20480 MiB (Fargate pairs cpu/memory ranges)
  description = "Fargate CPU units for the heavy-agency gtfs stage (GO_AHEAD only)."
}

variable "heavy_gtfs_memory" {
  type = string
  # First deploy of this task (2026-09-05) tried 16384 (stage_gtfs_memory's
  # ceiling, sized for the OTHER ~190 agencies) and GO_AHEAD OOMed the
  # container outright -- "OutOfMemoryError: container killed due to memory
  # usage", not a subprocess SIGKILL agency_batch could isolate. Went straight
  # to 20480 instead of guessing again: verified 2026-08-20 (see the old
  # rollup_heavy_memory var this replaces) that GO_AHEAD needs >8 GiB in
  # gtfs.py running ALONE, and 20 GiB was the ceiling that actually worked for
  # it in that combined task. Same number, now dedicated to gtfs alone instead
  # of shared with cold-ship/rollup/hot-ship in the same run.
  default     = "20480"
  description = "Fargate memory (MiB) for the heavy-agency gtfs stage (GO_AHEAD only)."
}

variable "heavy_snapshot_cpu" {
  type        = string
  default     = "4096" # 4 vCPU -- paired tier for 20480 MiB (Fargate pairs cpu/memory ranges)
  description = "Fargate CPU units for the heavy-agency snapshot stage (BKK/EDMONTON_TRANSIT_SYSTEM/LONDON_TRANSIT_COMMISSION/VBB)."
}

variable "heavy_snapshot_memory" {
  type        = string
  default     = "20480" # 20 GiB
  description = <<-EOT
    Fargate memory (MiB) for the heavy-agency snapshot stage.
    analysis.alert_snapshot.build_alert_snapshot buffers a whole day of raw
    payloads in one list -- BKK alone is ~13 GB/day of raw on bkk-trips. Matches
    the old rollup_heavy ceiling, now dedicated to snapshot alone instead of
    shared with gtfs/gold/rollup in the same run.
  EOT
}

variable "heavy_gold_cpu" {
  type        = string
  default     = "4096" # 4 vCPU -- paired tier for 24576 MiB
  description = "Fargate CPU units for the heavy-agency gold stage (METRO_HOUSTON/CINCINNATI_METRO/URBAN_MOBILITY_CENTER_SOFIA_TRAFFIC)."
}

variable "heavy_gold_memory" {
  type        = string
  default     = "24576" # 24 GiB
  description = <<-EOT
    Fargate memory (MiB) for the heavy-agency gold stage. Matches
    stage_gold_memory exactly -- same failure mode (gold.py), same reasoning:
    24 GiB here costs nothing on the other heavy stages' ceilings.
  EOT
}

variable "heavy_stage_workers" {
  type        = number
  default     = 4
  description = "Agencies processed concurrently within the heavy_rollup stage only (the other three heavy stages stay --workers 1, same isolation reasoning as the old rollup_heavy)."
}

# --- Per-stage tasks (stages.tf) ------------------------------------------ #
# Phase 1 of the stage split. Every size here is a generous first guess, NOT a
# measured value -- the whole point of splitting is that they can now be tuned
# independently, so watch pipeline.stage_<name>.duration and the task memory
# graphs for a week before cutting any of them. Fargate constrains which
# cpu/memory pairs are legal: 2048 CPU allows 4096-16384 MiB.

variable "stage_gtfs_cpu" {
  type        = string
  default     = "2048" # gtfs is memory-bound, not CPU-bound: parse + write marts
  description = "Fargate CPU units for the gtfs stage task."
}

variable "stage_gtfs_memory" {
  type        = string
  default     = "16384"
  description = <<-EOT
    Fargate memory (MiB) for the gtfs stage task. GO_AHEAD needs >8 GiB in
    gtfs.py running ALONE (verified 2026-08-20), but it is in
    local.heavy_agencies and therefore excluded here -- this ceiling only has to
    cover the rest of the fleet running --workers wide.
  EOT
}

variable "stage_snapshot_cpu" {
  type        = string
  default     = "2048"
  description = "Fargate CPU units for the snapshot stage task."
}

variable "stage_snapshot_memory" {
  type        = string
  default     = "16384"
  description = <<-EOT
    Fargate memory (MiB) for the snapshot stage task. snapshot.py accounted for
    25 of the 42 SIGKILLs in the month to 2026-09-03 because
    analysis.alert_snapshot.build_alert_snapshot buffers a whole day of raw
    payloads in one list. The worst offenders (BKK, EDMONTON, LTC, VBB) are in
    local.heavy_agencies and excluded here, but until that buffer is fixed this
    stage stays the most likely one to need headroom.
  EOT
}

variable "stage_archive_cpu" {
  type        = string
  default     = "2048" # gzip during tarball build is the only real CPU here
  description = "Fargate CPU units for the archive (cold-ship + prune) stage task."
}

variable "stage_archive_memory" {
  type        = string
  default     = "8192"
  description = <<-EOT
    Fargate memory (MiB) for the archive stage task. Shipper._build_tarball
    streams one landing object at a time rather than materializing a day, so
    peak is roughly (workers x largest single hourly bin), not a day's volume.
  EOT
}

variable "stage_gold_cpu" {
  type = string
  # 4096, not 2048: Fargate only allows 4096-16384 MiB against 2048 CPU, and
  # stage_gold_memory is 24576. The memory tier is what's being bought here --
  # the extra vCPU comes along because Fargate pairs the ranges.
  default     = "4096"
  description = "Fargate CPU units for the gold stage task."
}

variable "stage_gold_memory" {
  type        = string
  default     = "24576"
  description = <<-EOT
    Fargate memory (MiB) for the gold stage task -- the headroom that motivated
    the whole split. gold.py accounted for 17 of the 42 SIGKILLs in the month to
    2026-09-03, and in the combined task it could only be given more memory by
    giving rollup the same. 24 GiB here costs nothing on the rollup side.
  EOT
}

variable "stage_workers" {
  type        = number
  default     = 4
  description = "Agencies processed concurrently within each per-stage task (agency_batch --workers)."
}

variable "stage_schedule_enabled" {
  type        = bool
  default     = false
  description = <<-EOT
    Whether the daily schedules for the per-stage tasks are ENABLED. Applied
    disabled on purpose: verify each stage with a manual run-task first, and
    flip this in the SAME apply that removes the corresponding stages from the
    main rollup task, or the work is either done twice or not at all. See
    rollup_heavy_schedule_enabled for what happens when those two drift apart.
  EOT
}

variable "stage_schedule_expression" {
  type        = string
  default     = "cron(30 3 * * ? *)" # same slot: these stages are independent of each other and of rollup
  description = "EventBridge Scheduler expression (UTC) for the per-stage tasks."
}

variable "rollup_ephemeral_storage_gib" {
  type = number
  # Fargate default is 20 GiB when unset. A same-day manual backfill run for
  # 2026-08-15 (task c6164db1, run overnight 2026-08-16 to recover the 3 days
  # of missing hot data from the OOM incident above) hit that ceiling instead:
  # "[go-ahead-vehicles-1797 2026-08-15] SKIP — [Errno 28] No space left on
  # device: 'data/curated/snapshots'" ~81 min in. curated_dir (landing bins +
  # curated parquet + GTFS snapshots) is local disk shared by every step in
  # this one combined task (see the Step Functions revert note above), so more
  # feeds means more of all three piling up before ship.py clears any of it.
  # Doubling to 40, same stopgap logic as rollup_memory: buys headroom, not a
  # measured requirement.
  default     = 40
  description = "Fargate task ephemeral storage (GiB) for the rollup stage. Must be 21-200; 20 is the unconfigured default."
}

variable "env_secret_name" {
  type        = string
  default     = "rail-archiver/env"
  description = "Secrets Manager secret holding the agency API keys as a JSON object (populated out-of-band, not by Terraform)."
}

variable "log_retention_days" {
  type    = number
  default = 14
}

# --- Aux tasks split out of the rollup task -------------------------------- #
# cert_check and s3_storage_metrics used to run as extra steps inside the
# rollup container (see the removed lines in rollup.tf's locals.rollup_script).
# Neither touches curated_dir or takes --day, so they don't need the rollup's
# shared local disk and can run on their own schedule instead. Same cautious
# rollout as rollup_schedule_enabled: start disabled, verify with a manual
# run-task, then flip to true.

variable "cert_check_schedule_enabled" {
  type        = bool
  default     = false
  description = "Whether the daily EventBridge schedule for the standalone cert_check task is ENABLED."
}

variable "cert_check_schedule_expression" {
  type        = string
  default     = "cron(0 5 * * ? *)"
  description = "EventBridge Scheduler expression (UTC) for the daily cert-expiry probe. No longer tied to the rollup window since it shares nothing with it."
}

variable "s3_storage_metrics_schedule_enabled" {
  type        = bool
  default     = false
  description = "Whether the daily EventBridge schedule for the standalone s3_storage_metrics task is ENABLED."
}

variable "s3_storage_metrics_schedule_expression" {
  type        = string
  default     = "cron(15 5 * * ? *)"
  description = "EventBridge Scheduler expression (UTC) for the daily S3 storage-cost metrics. CloudWatch's BucketSizeBytes only updates once/day, so once is enough; offset a few minutes from cert_check just to keep their log streams from interleaving."
}

variable "historic_511_schedule_enabled" {
  type        = bool
  default     = false
  description = "Whether the monthly EventBridge schedule for the historic_511 task is ENABLED. Same cautious rollout as the other schedule vars: apply disabled, verify with a manual run-task, then flip to true."
}

variable "historic_511_schedule_expression" {
  type        = string
  default     = "cron(0 6 5 * ? *)"
  description = "EventBridge Scheduler expression (UTC) for the monthly 511.org historic-archive pull — 5th of the month, giving 511 a few days' buffer to publish the prior month's archive."
}

# --- Cost monitoring -------------------------------------------------------- #

variable "cost_alert_email" {
  type        = string
  description = "Email address subscribed to AWS Budgets / Cost Anomaly Detection alerts (cost_monitoring.tf). AWS sends a confirmation email after apply that must be clicked before delivery starts."
}

variable "monthly_cost_budget_usd" {
  type        = string
  default     = "50"
  description = "Placeholder monthly budget (USD) for this project's tagged spend. The account is shared with other projects, so there's no real baseline yet — revisit after the first month of cost-allocation-tag data."
}

variable "cost_anomaly_threshold_usd" {
  type        = string
  default     = "10"
  description = "Minimum dollar impact for a Cost Anomaly Detection alert to fire, so small noise doesn't page."
}
