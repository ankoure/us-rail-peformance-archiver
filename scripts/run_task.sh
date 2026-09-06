#!/usr/bin/env bash
# Manual/backfill ECS RunTask wrapper.
#
# Tags every task it starts trigger=manual, so it's distinguishable in Cost
# Explorer / terraform/cost_monitoring.tf from EventBridge Scheduler runs
# (tagged trigger=scheduled in terraform/aux_schedule.tf and
# terraform/rollup_schedule.tf). Without this, a deliberate backfill and a
# runaway job look identical in cost data.
#
# Cluster/subnets/security-group are read from `terraform output`, so they
# never drift from what's actually deployed.
#
# Usage:
#   scripts/run_task.sh <task-family> [--day YYYY-MM-DD] [--spot] [--profile PROFILE]
#
# --day sets ROLLUP_DAY on the task's main container, which is how every
# agency_batch-driven task targets a past day instead of its default
# `${ROLLUP_DAY:-$(date -u -d yesterday +%F)}` (terraform/rollup.tf,
# terraform/stages.tf, terraform/heavy_stages.tf). Re-runs are safe: ship.py
# skips any object already in S3 unless --force, so a day that only partly
# shipped fills in its gaps.
#
# Every agency_batch-driven task is single-stage (or a fixed sub-chain) by
# construction now -- there is no longer a task that reads a stages override
# at runtime (rollup_heavy, retired 2026-09-05, was the last one).
#
# Examples:
#   scripts/run_task.sh rail-archiver-rollup --profile KourePowerUser
#   scripts/run_task.sh rail-archiver-heavy-gold --day 2026-08-27 --profile KourePowerUser
#   scripts/run_task.sh rail-archiver-stage-gold --day 2026-09-04 --profile KourePowerUser
#   scripts/run_task.sh rail-archiver-historic-511-otp --spot --profile KourePowerUser
#
# Task families (see terraform/*.tf): rail-archiver-rollup,
# rail-archiver-stage-{gtfs,snapshot,archive,gold},
# rail-archiver-heavy-{rollup,gtfs,snapshot,gold}, rail-archiver-cert-check,
# rail-archiver-s3-storage-metrics, rail-archiver-historic-511,
# rail-archiver-historic-511-otp, rail-archiver-gold-backfill.

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <task-family> [--day YYYY-MM-DD] [--spot] [--profile PROFILE]" >&2
  exit 1
fi

task_family="$1"
shift

spot=false
day=""
profile_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --day)
      day="$2"
      shift 2
      ;;
    --spot)
      spot=true
      shift
      ;;
    --profile)
      profile_args=(--profile "$2")
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tf_dir="$script_dir/../terraform"

cluster=$(terraform -chdir="$tf_dir" output -raw rollup_cluster)
subnets=$(terraform -chdir="$tf_dir" output -json rollup_subnet_ids | jq -r 'join(",")')
security_group=$(terraform -chdir="$tf_dir" output -raw rollup_security_group)

env_json="[]"

add_env() {
  env_json=$(jq -c --arg n "$1" --arg v "$2" '. + [{name: $n, value: $v}]' <<<"$env_json")
}

if [[ -n "$day" ]]; then
  [[ "$day" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || {
    echo "--day must be YYYY-MM-DD, got: $day" >&2
    exit 1
  }
  add_env ROLLUP_DAY "$day"
fi

# The container to override is the task's own, not the datadog-agent sidecar.
# Names are set in the task definitions and follow the family minus its
# "rail-archiver-" prefix (rollup, stage-gold, heavy-gold, ...).
override_args=()
if [[ "$env_json" != "[]" ]]; then
  container="${task_family#rail-archiver-}"
  override_args=(--overrides "$(jq -cn --arg c "$container" --argjson e "$env_json" \
    '{containerOverrides: [{name: $c, environment: $e}]}')")
fi

launch_args=(--launch-type FARGATE)
if $spot; then
  # aws ecs run-task rejects --launch-type FARGATE_SPOT outright; Spot is only
  # selectable via --capacity-provider-strategy (reference_ecs_spot_run_task
  # memory — hit this twice writing 511 backfill commands by hand).
  launch_args=(--capacity-provider-strategy capacityProvider=FARGATE_SPOT,weight=1)
fi

aws ecs run-task \
  "${profile_args[@]}" \
  --cluster "$cluster" \
  --task-definition "$task_family" \
  --count 1 \
  "${launch_args[@]}" \
  "${override_args[@]+"${override_args[@]}"}" \
  --network-configuration "awsvpcConfiguration={subnets=[$subnets],securityGroups=[$security_group],assignPublicIp=ENABLED}" \
  --tags key=trigger,value=manual
