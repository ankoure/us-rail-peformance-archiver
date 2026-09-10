# --- Downsized poller box (Phase E) --------------------------------------- #
# Replaces the hand-built t3.large (x86, 500 GiB) with a Terraform-managed
# t4g.small (ARM, 30 GiB root only). The rollup moved to Fargate and the landing
# moved to S3, so the box is now poller-only: ~0.1 core of I/O-bound polling plus
# the in-process LandingUploader. No data volume — local landing is just the
# transient outbox the uploader drains and deletes.
#
# user_data bootstraps everything EXCEPT `.env` (pasted out-of-band via an SSM
# session) and the `docker compose up` (held until .env exists, to avoid the new
# box dual-polling the live one during cutover). A marker file signals readiness.
#
# That manually-pasted .env MUST include `CONTINENT=us` (see box_eu.tf/box_au.tf
# for the same flag on those boxes) -- without it this box falls back to
# unrestricted (polls/lands every agency globally, not just US), duplicating
# every EU/AU agency's landing writes against the regional boxes. Not enforced
# by terraform since the whole file is pasted out-of-band; a future box
# replacement needs a human to remember this, same as the prune-timer lesson
# above.

variable "poller_instance_type" {
  type    = string
  default = "t4g.small" # ARM/Graviton, 2 vCPU / 2 GiB -- downsized 2026-09-10: live
  # telemetry showed ~700MB/3.7GB RAM used and load avg <0.4 even at 148 US
  # agencies (post CONTINENT=us narrowing), so the t4g.medium bump from
  # 2026-08-14 (149a21f, added for then-more-agencies load) has real headroom
  # to give back. The historical 3x OOM crash-loop was fixed via the DELFI
  # feed removal + per-poll entity-count walk in archiver code, not by
  # instance size -- those guards should hold regardless of box size. No
  # CloudWatch memory metric exists on this box, so watch it for a few days
  # after this change.
}

variable "poller_root_gb" {
  type = number
  # Manually resized 30 -> 50 on 2026-08-20 (EC2 console/CLI, out of band) to
  # recover from the local-landing-fills-disk outage; this default was never
  # updated to match, so `terraform plan` wanted to shrink it back to 30 --
  # which EBS can't even do via ModifyVolume. Keep this in sync with the live
  # volume size going forward.
  default = 50
}

variable "poller_subnet_id" {
  type        = string
  default     = "subnet-0fc4958b243ac7bdc" # default-VPC public subnet, us-east-1a
  description = "Public subnet (MapPublicIpOnLaunch) so the poller reaches feeds/S3/GHCR."
}

variable "poller_security_group_id" {
  type        = string
  default     = "sg-01d9a0cd549f0c197" # the existing box's egress-only SG (reused)
  description = "Egress-only SG (no inbound; SSM needs none)."
}

# Always the latest AL2023 arm64 AMI (SSM-agent preinstalled).
data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}

locals {
  poller_user_data = <<-EOT
    #!/bin/bash
    set -euxo pipefail
    # Everything here runs as root under cloud-init. NOTE: do NOT reference
    # ssm-user — the SSM agent creates it lazily on first session connect, so it
    # does not exist at boot (an earlier `usermod ssm-user` aborted the script).
    dnf install -y docker git
    systemctl enable --now docker

    # The base AL2023 `docker` package ships the engine but NOT the compose v2
    # plugin, so `docker compose` just prints help. Install the arm64 plugin.
    mkdir -p /usr/libexec/docker/cli-plugins
    curl -sSL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-aarch64 \
      -o /usr/libexec/docker/cli-plugins/docker-compose
    chmod +x /usr/libexec/docker/cli-plugins/docker-compose

    mkdir -p /opt/rail-archiver
    cd /opt/rail-archiver
    git clone https://github.com/ankoure/us-rail-performance-archiver.git .

    # The poller containers run as uid 1000 and bind-mount ./archive (landing
    # outbox) and ./poll_state/shard-N; pre-create them owned by 1000 (config is
    # mounted read-only). Avoid `make` — not installed on a base AL2023.
    mkdir -p poll_state/shard-0 poll_state/shard-1 archive
    chown -R 1000:1000 poll_state archive

    # 4 GiB swap on root (no data volume now). Safety net for any memory spike in
    # the poller; low swappiness so it's not eager. dd, not fallocate (see README).
    dd if=/dev/zero of=/swapfile bs=1M count=4096
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo "/swapfile none swap sw 0 0" >> /etc/fstab
    echo "vm.swappiness=10" > /etc/sysctl.d/99-swappiness.conf
    sysctl --system

    # Local landing prune, daily. Without this the on-box landing zone (raw
    # window .bin files + the local-only daily data.jsonl -- see
    # landing_layout.py, which deliberately excludes data.jsonl from the S3
    # upload scan) grows unbounded and eventually fills the disk: this is what
    # caused the 2026-08-20 outage, after which this timer was hand-added via
    # SSM session and NOT captured in terraform -- so a box replacement would
    # silently lose it and reproduce the same outage. --keep-days 3 only
    # deletes a day-partition once its cold tarball is confirmed in S3, so
    # it's crash-safe; it no-ops harmlessly (container not up yet) until the
    # operator's first `docker compose up`.
    # printf line-by-line (not a nested heredoc): this script's own <<-EOT
    # wrapper strips leading indentation by an amount tied to ITS closing
    # marker, so a nested heredoc's closing line would land at an unpredictable
    # column and could fail to match -- printf has no such indentation hazard.
    printf '%s\n' \
      '[Unit]' \
      'Description=Prune local rail-archiver landing zone (deletes only S3-cold-confirmed day-partitions)' \
      'After=docker.service' \
      'Requires=docker.service' \
      '' \
      '[Service]' \
      'Type=oneshot' \
      'ExecStart=/usr/bin/docker exec rail-archiver-app-shard-0-1 python pipeline/prune.py --keep-days 3 -v' \
      > /etc/systemd/system/rail-archiver-prune.service

    printf '%s\n' \
      '[Unit]' \
      'Description=Daily rail-archiver local landing zone prune' \
      '' \
      '[Timer]' \
      'OnCalendar=daily' \
      'RandomizedDelaySec=15m' \
      'Persistent=true' \
      '' \
      '[Install]' \
      'WantedBy=timers.target' \
      > /etc/systemd/system/rail-archiver-prune.timer

    systemctl daemon-reload
    systemctl enable --now rail-archiver-prune.timer

    # Bootstrap done. Pollers are NOT started here: that waits until .env is
    # added (SSM session, with sudo) and the operator runs `docker compose up`,
    # so the new box doesn't dual-poll the old one mid-cutover.
    touch /opt/rail-archiver/.bootstrap-complete
  EOT
}

resource "aws_instance" "poller" {
  ami                         = data.aws_ssm_parameter.al2023_arm64.value
  instance_type               = var.poller_instance_type
  subnet_id                   = var.poller_subnet_id
  vpc_security_group_ids      = [var.poller_security_group_id]
  iam_instance_profile        = var.instance_role_name # rail-archiver-instance
  associate_public_ip_address = true
  user_data                   = local.poller_user_data

  root_block_device {
    volume_size = var.poller_root_gb
    volume_type = "gp3"
    encrypted   = true
  }

  metadata_options {
    http_tokens = "required" # IMDSv2 only
  }

  lifecycle {
    # data.aws_ssm_parameter.al2023_arm64 tracks "latest" — without this, every
    # plan run after AWS publishes a new AL2023 AMI wants to replace the LIVE
    # box. Replacement means real downtime (pollers don't auto-start; .env has
    # to be re-pasted via SSM session and `docker compose up` run by hand) plus
    # a new public IP. Ignore drift here; pick up a new AMI via an explicit,
    # intentional replacement (e.g. `terraform taint`), not silently.
    ignore_changes = [ami]
  }

  tags = {
    Name = "rail-archiver-poller"
    # Required: the deploy role's ssm:SendCommand is scoped to instances tagged
    # Application=rail-archiver, so deploy.yml can only reach the box if it's tagged.
    Application = "rail-archiver"
  }
}

output "poller_instance_id" {
  value = aws_instance.poller.id
}

output "poller_public_ip" {
  value = aws_instance.poller.public_ip
}
