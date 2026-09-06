# --- Daily trigger: EventBridge Scheduler -> ECS RunTask on Fargate Spot --- #
# Replaces the on-box `batch` loop. Created DISABLED (var.rollup_schedule_enabled)
# so the first prod run is the manual, verified run-task in Phase D; flip the var
# to true and re-apply once that run checks out.

data "aws_caller_identity" "current" {}

# Role the scheduler assumes to launch the task.
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "rollup_scheduler" {
  name               = "rail-archiver-rollup-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "rollup_scheduler" {
  name = "run-rollup-task"
  role = aws_iam_role.rollup_scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RunTask"
        Effect = "Allow"
        Action = ["ecs:RunTask"]
        # Any revision of any family, so re-registering a task def (new apply)
        # doesn't require re-granting. Every un-sequenced (plain-cron) task
        # that reuses this role goes here: the main rollup, regular stage-gtfs,
        # and heavy-gtfs/heavy-snapshot (heavy_stages.tf) -- all conceptually
        # part of the rollup family, not an aux task (contrast
        # aux_schedule.tf's cert_check/etc.). Tasks the state machine
        # sequences (stage-gold/-snapshot/-archive, heavy-rollup/-gold) are
        # granted on aws_iam_role.sfn instead (stage_orchestration.tf), since
        # THAT role is what actually calls RunTask for them.
        #
        # stage-gtfs was missing from this list from 2026-09-04 (when it was
        # created) until 2026-09-05 -- var.stage_schedule_enabled had never
        # been flipped on in that window, so its cron never fired and the gap
        # went uncaught. Caught by a manual run-task under root credentials,
        # which bypasses this role entirely and so didn't exercise it either;
        # only a scheduled firing under THIS role would have.
        Resource = [
          "arn:aws:ecs:${var.region}:${data.aws_caller_identity.current.account_id}:task-definition/${aws_ecs_task_definition.rollup.family}:*",
          "arn:aws:ecs:${var.region}:${data.aws_caller_identity.current.account_id}:task-definition/${aws_ecs_task_definition.stage["gtfs"].family}:*",
          "arn:aws:ecs:${var.region}:${data.aws_caller_identity.current.account_id}:task-definition/${aws_ecs_task_definition.heavy_stage["gtfs"].family}:*",
          "arn:aws:ecs:${var.region}:${data.aws_caller_identity.current.account_id}:task-definition/${aws_ecs_task_definition.heavy_stage["snapshot"].family}:*",
        ]
        Condition = {
          ArnLike = { "ecs:cluster" = aws_ecs_cluster.main.arn }
        }
      },
      {
        # RunTask passes the task + execution roles to ECS on the task's behalf.
        Sid      = "PassTaskRoles"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [aws_iam_role.rollup_task.arn, aws_iam_role.rollup_execution.arn]
      },
      {
        # RunTask with ecs_parameters.tags set (added for scheduled-vs-manual
        # cost tracking) requires ecs:TagResource too — AWS treats tagging the
        # task being created as a separate permission check from RunTask
        # itself. Missing this broke every scheduled rollup run outright
        # (AccessDenied on ecs:TagResource, confirmed via CloudTrail for the
        # 2026-08-20 03:30 UTC run — the rollup never started that night).
        Sid      = "TagTaskOnRun"
        Effect   = "Allow"
        Action   = ["ecs:TagResource"]
        Resource = ["arn:aws:ecs:${var.region}:${data.aws_caller_identity.current.account_id}:task/${aws_ecs_cluster.main.name}/*"]
      },
    ]
  })
}

resource "aws_scheduler_schedule" "rollup" {
  name = "rail-archiver-rollup-daily"
  # Once the stage split is live, the nightly state machine
  # (stage_orchestration.tf) is what invokes the rollup task -- it has to, since
  # it must wait for rollup before starting gold. This schedule must therefore
  # switch OFF at the same moment, or rollup runs twice a night. Same single
  # flag as the rest of the handover so the two can't drift apart.
  state = (
    var.rollup_schedule_enabled && !var.stage_schedule_enabled
    ? "ENABLED"
    : "DISABLED"
  )

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.rollup_schedule_expression
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_ecs_cluster.main.arn
    role_arn = aws_iam_role.rollup_scheduler.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.rollup.arn
      task_count          = 1
      # Distinguishes scheduled runs from manual/backfill run-task invocations
      # in Cost Explorer — see scripts/run_task.sh for the manual side.
      tags = { trigger = "scheduled" }
      # On-demand, NOT FARGATE_SPOT: the rollup reads the day from S3 object by
      # object (~2.5h) and EventBridge fires once daily with no retry, so a Spot
      # reclaim mid-run would silently drop that day. On-demand for ~2.5h/day is
      # a few $/mo — cheap insurance for a job that must complete.
      launch_type = "FARGATE"

      # Same networking the run-task SG comment assumes: default public subnets,
      # the egress-only rollup SG, and a public IP so the task reaches S3 + GHCR.
      network_configuration {
        subnets          = data.aws_subnets.default.ids
        security_groups  = [aws_security_group.rollup.id]
        assign_public_ip = true
      }
    }
  }
}
