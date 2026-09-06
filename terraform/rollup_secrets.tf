# --- Scratch hot bucket (shadow rollup output) ---------------------------- #
# The shadow rollup writes parquet HERE, not to prod hot, so we can diff the
# two without risk. Short retention — it's throwaway validation data.

resource "aws_s3_bucket" "hot_scratch" {
  bucket = var.hot_scratch_bucket
}

resource "aws_s3_bucket_public_access_block" "hot_scratch" {
  bucket                  = aws_s3_bucket.hot_scratch.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "hot_scratch" {
  bucket = aws_s3_bucket.hot_scratch.id
  rule {
    id     = "expire-scratch"
    status = "Enabled"
    filter {}
    expiration {
      days = var.hot_scratch_retention_days
    }
  }
}

# --- Env secret (agency API keys + regional poller .env) ------------------ #
# Terraform creates the secret CONTAINER only; the value is put out-of-band so
# secrets never land in TF state/code.
#
# Three consumers now read this SAME secret, none wanting the whole thing:
#   - The Fargate rollup task (rollup_iam.tf, rollup.tf) -- reads ONLY
#     DD_API_KEY (hardcoded on the datadog-agent container def). It never
#     builds a live feed client (archiver.loader.build_feeds() is metadata-
#     only), so it has no use for -- and, as of this file, no longer even
#     requests -- any agency key. Extra keys below are simply invisible to it.
#   - Each poller box's user_data (box_eu.tf/box_au.tf; box.tf's original
#     us-east-1 box still gets .env pasted by hand per deploy/README.md) --
#     fetches the blob at boot and filters it to that box's own continent
#     (scripts/agency_env_keys.py) plus the always-keep infra keys, via the
#     box_secrets_read policy (landing.tf). pipeline/refresh_env.py replays
#     the same fetch, unfiltered, when pushing a rotated/new key afterward --
#     see its TODO to apply the same continent filter.
#   - The gtfs/gold stage tasks (stages.tf, heavy_stages.tf) -- read
#     MDB_REFRESH_TOKEN (2026-09-06) to authenticate against
#     api.mobilitydatabase.org for static-GTFS resolution (analysis/
#     gtfs_fetcher.py). This is ONE MobilityDatabase-account-level credential,
#     not per-agency -- it isn't derived from config/feeds.yaml's auth blocks
#     at all, so scripts/agency_env_keys.py won't pick it up automatically;
#     it has to stay in the hardcoded infra-keys list below.
#
# So the value must be a flat JSON object of every agency's auth env var
# (config/feeds.yaml) PLUS DD_API_KEY, DD_SITE, AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY, MDB_REFRESH_TOKEN (the poller's full un-filtered
# .env, minus continent/shard/hostname -- those are per-box, baked into
# user_data directly, not secret). Deliberately does NOT include every key
# that might be in a local dev .env (e.g. MBTA_API_KEY, DD_APP_KEY,
# POSTGIS_DATABASE_URL) -- only what config/feeds.yaml's auth blocks and
# compose.prod.yml actually reference, plus the hardcoded infra keys below.
# The key LIST used to be hand-maintained here and drifted stale more than
# once (missing TFNSW/PTV/AT_METRO/METRO_CHRISTCHURCH keys each broke
# something before anyone noticed) -- scripts/agency_env_keys.py (no
# --continent) now computes the agency-key part from config/feeds.yaml
# directly, so use that instead of retyping a list:
#
#   aws secretsmanager put-secret-value --secret-id rail-archiver/env \
#     --secret-string "$(python3 - <<'PY'
#   import json, subprocess
#   keys = subprocess.run(
#       ["uv", "run", "python", "scripts/agency_env_keys.py"],
#       capture_output=True, text=True, check=True,
#   ).stdout.split()
#   keys += ["DD_API_KEY", "DD_SITE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "MDB_REFRESH_TOKEN"]
#   env = dict(l.strip().split("=",1) for l in open(".env") if "=" in l and not l.startswith("#"))
#   print(json.dumps({k: env[k] for k in keys if k in env}))
#   PY
#   )"

resource "aws_secretsmanager_secret" "env" {
  name                    = var.env_secret_name
  description             = "Agency API keys + poller .env (JSON) -- read by the Fargate rollup task and each poller box's boot-time user_data."
  recovery_window_in_days = 0 # allow immediate delete/recreate while iterating
}
