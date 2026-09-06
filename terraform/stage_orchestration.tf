# --- Step Functions: the two real dependency edges (phase 2) --------------- #
#
# gtfs reads nothing any other stage writes, so it stays plainly scheduled
# (stages.tf) and needs no orchestration. Two pairs do have a real ordering
# requirement, each handled as its own branch of the Nightly Parallel state
# below -- the branches themselves are independent of each other and run
# concurrently:
#
#   Rollup -> Gold: gold reads rollup's silver, so it must not start until
#   rollup has finished AND shipped.
#
#   Snapshot -> Archive: archive's post-step is prune_s3, which deletes a
#   landing day-partition once its cold tarball is confirmed shipped. Cold-ship
#   used to run inside the same monolithic task as snapshot, after it, so a day
#   was never eligible for pruning before snapshot had already had its chance
#   to read that day's raw payloads. Splitting them into separate tasks removes
#   that implicit ordering -- archive could otherwise run before snapshot ever
#   starts. prune_s3 (archiver/shipper.py) now ALSO checks that an alerts-
#   capable feed has a snapshot object in S3 before deleting its landing data,
#   which is the correctness backstop for a feed that fails snapshot every
#   single night (see local.heavy_agencies in rollup.tf) -- ordering alone
#   can't help *that* case, since archive would just keep finding no snapshot
#   forever. This edge closes the narrower, cheaper-to-hit case: archive
#   racing ahead of a snapshot run that would otherwise have succeeded.
#
# Note the 2026-07-31 attempt used Step Functions too and was reverted, but the
# orchestration was never the problem: the problem was that gold and rollup ran
# in separate tasks with separate ephemeral disks and no way to share curated
# data, so gold saw an empty tree and silently no-op'd. That is fixed two ways
# now, both of which have to hold before this is safe to enable:
#   1. gold reads silver over S3 from the hot bucket (analysis/curated_fs.py),
#      so it no longer needs rollup's local disk at all; and
#   2. gold.assert_silver_present() makes an empty tree a non-zero exit instead
#      of a zero-row success, so the same mistake can't be silent twice.
# Snapshot -> Archive has no such hazard: archive's cold-ship reads landing
# (S3), not curated/, so it was never exposed to the shared-local-disk problem.
#
# ROLLUP_DAY is threaded through the execution input so a manual re-run of one
# past day drives all four tasks consistently: start an execution with
# {"day": "2026-09-02"} rather than running them by hand.

locals {
  # Both branches override ROLLUP_DAY from the execution input when present.
  # States.Format with a null-ish input would produce "None", so the default
  # empty string is resolved in-container by the script's `${ROLLUP_DAY:-...}`.
  # Both pairs are Task/.sync -> Task/.sync chains with an identical shape
  # (override ROLLUP_DAY, discard the ECS result so $.day survives, catch any
  # failure and proceed anyway), so the two branches below are structurally
  # twins -- see the header comment for what each edge is actually for.
  sfn_definition = jsonencode({
    Comment = "rail-archiver nightly: rollup -> gold and snapshot -> archive, running as independent parallel branches"
    StartAt = "Nightly"
    States = {
      Nightly = {
        Type = "Parallel"
        # Branches run concurrently and are independent of each other --
        # gold's dependency is only on rollup, archive's only on snapshot.
        Branches = [
          {
            StartAt = "Rollup"
            States = {
              Rollup = {
                Type     = "Task"
                Resource = "arn:aws:states:::ecs:runTask.sync"
                Parameters = {
                  Cluster        = aws_ecs_cluster.main.arn
                  TaskDefinition = aws_ecs_task_definition.rollup.arn
                  LaunchType     = "FARGATE"
                  NetworkConfiguration = {
                    AwsvpcConfiguration = {
                      Subnets        = data.aws_subnets.default.ids
                      SecurityGroups = [aws_security_group.rollup.id]
                      AssignPublicIp = "ENABLED"
                    }
                  }
                  Overrides = {
                    ContainerOverrides = [{
                      Name            = "rollup"
                      "Environment.$" = "States.Array(States.StringToJson(States.Format('{{\"Name\":\"ROLLUP_DAY\",\"Value\":\"{}\"}}', $.day)))"
                    }]
                  }
                }
                # A Task's DEFAULT ResultPath is "$", i.e. the ECS result
                # REPLACES the state input -- which would drop $.day before
                # Gold ever reads it, and only at runtime. null discards the
                # result and passes the input through unchanged. (The Catch
                # below already preserves it, so without this the failure path
                # would work and the success path would not.)
                ResultPath = null

                # A failed agency makes the task exit non-zero, but that must
                # NOT stop gold: the other ~190 agencies rolled up fine and
                # their marts are still worth building. Catch and continue,
                # exactly like agency_batch's own per-agency isolation one
                # level down.
                Catch = [{
                  ErrorEquals = ["States.ALL"]
                  Next        = "Gold"
                  ResultPath  = "$.rollupError"
                }]
                Next = "Gold"
              }
              Gold = {
                Type     = "Task"
                Resource = "arn:aws:states:::ecs:runTask.sync"
                Parameters = {
                  Cluster        = aws_ecs_cluster.main.arn
                  TaskDefinition = aws_ecs_task_definition.stage["gold"].arn
                  LaunchType     = "FARGATE"
                  NetworkConfiguration = {
                    AwsvpcConfiguration = {
                      Subnets        = data.aws_subnets.default.ids
                      SecurityGroups = [aws_security_group.rollup.id]
                      AssignPublicIp = "ENABLED"
                    }
                  }
                  Overrides = {
                    ContainerOverrides = [{
                      Name            = "stage-gold"
                      "Environment.$" = "States.Array(States.StringToJson(States.Format('{{\"Name\":\"ROLLUP_DAY\",\"Value\":\"{}\"}}', $.day)))"
                    }]
                  }
                }
                End = true
              }
            }
          },
          {
            StartAt = "Snapshot"
            States = {
              Snapshot = {
                Type     = "Task"
                Resource = "arn:aws:states:::ecs:runTask.sync"
                Parameters = {
                  Cluster        = aws_ecs_cluster.main.arn
                  TaskDefinition = aws_ecs_task_definition.stage["snapshot"].arn
                  LaunchType     = "FARGATE"
                  NetworkConfiguration = {
                    AwsvpcConfiguration = {
                      Subnets        = data.aws_subnets.default.ids
                      SecurityGroups = [aws_security_group.rollup.id]
                      AssignPublicIp = "ENABLED"
                    }
                  }
                  Overrides = {
                    ContainerOverrides = [{
                      Name            = "stage-snapshot"
                      "Environment.$" = "States.Array(States.StringToJson(States.Format('{{\"Name\":\"ROLLUP_DAY\",\"Value\":\"{}\"}}', $.day)))"
                    }]
                  }
                }
                ResultPath = null

                # Same reasoning as Rollup's Catch: one agency SIGKILLing in
                # snapshot.py must not stop archive from pruning everyone
                # else's confirmed-shipped, confirmed-snapshotted days. The
                # agencies that never get a snapshot object are exactly the
                # ones prune_s3's new per-feed check now holds back on its own
                # (see archiver/shipper.py's prune_s3 docstring).
                Catch = [{
                  ErrorEquals = ["States.ALL"]
                  Next        = "Archive"
                  ResultPath  = "$.snapshotError"
                }]
                Next = "Archive"
              }
              Archive = {
                Type     = "Task"
                Resource = "arn:aws:states:::ecs:runTask.sync"
                Parameters = {
                  Cluster        = aws_ecs_cluster.main.arn
                  TaskDefinition = aws_ecs_task_definition.stage["archive"].arn
                  LaunchType     = "FARGATE"
                  NetworkConfiguration = {
                    AwsvpcConfiguration = {
                      Subnets        = data.aws_subnets.default.ids
                      SecurityGroups = [aws_security_group.rollup.id]
                      AssignPublicIp = "ENABLED"
                    }
                  }
                  Overrides = {
                    ContainerOverrides = [{
                      Name            = "stage-archive"
                      "Environment.$" = "States.Array(States.StringToJson(States.Format('{{\"Name\":\"ROLLUP_DAY\",\"Value\":\"{}\"}}', $.day)))"
                    }]
                  }
                }
                End = true
              }
            }
          },
          # Third branch, 2026-09-05: local.heavy_agencies' rollup -> gold edge
          # (heavy_stages.tf), the same real dependency as the first branch's
          # Rollup -> Gold, just for the agencies isolated out of that task.
          # heavy_gtfs and heavy_snapshot have no such dependency (same
          # reasoning as the regular gtfs/snapshot stages) so they stay on
          # plain crons in heavy_stages.tf rather than living here.
          {
            StartAt = "HeavyRollup"
            States = {
              HeavyRollup = {
                Type     = "Task"
                Resource = "arn:aws:states:::ecs:runTask.sync"
                Parameters = {
                  Cluster        = aws_ecs_cluster.main.arn
                  TaskDefinition = aws_ecs_task_definition.heavy_stage["rollup"].arn
                  LaunchType     = "FARGATE"
                  NetworkConfiguration = {
                    AwsvpcConfiguration = {
                      Subnets        = data.aws_subnets.default.ids
                      SecurityGroups = [aws_security_group.rollup.id]
                      AssignPublicIp = "ENABLED"
                    }
                  }
                  Overrides = {
                    ContainerOverrides = [{
                      Name            = "heavy-rollup"
                      "Environment.$" = "States.Array(States.StringToJson(States.Format('{{\"Name\":\"ROLLUP_DAY\",\"Value\":\"{}\"}}', $.day)))"
                    }]
                  }
                }
                ResultPath = null

                # Same reasoning as the regular branch's Rollup Catch: a
                # heavy agency's rollup failure must not also cost heavy_gold
                # a chance to run for the OTHER heavy agencies.
                Catch = [{
                  ErrorEquals = ["States.ALL"]
                  Next        = "HeavyGold"
                  ResultPath  = "$.heavyRollupError"
                }]
                Next = "HeavyGold"
              }
              HeavyGold = {
                Type     = "Task"
                Resource = "arn:aws:states:::ecs:runTask.sync"
                Parameters = {
                  Cluster        = aws_ecs_cluster.main.arn
                  TaskDefinition = aws_ecs_task_definition.heavy_stage["gold"].arn
                  LaunchType     = "FARGATE"
                  NetworkConfiguration = {
                    AwsvpcConfiguration = {
                      Subnets        = data.aws_subnets.default.ids
                      SecurityGroups = [aws_security_group.rollup.id]
                      AssignPublicIp = "ENABLED"
                    }
                  }
                  Overrides = {
                    ContainerOverrides = [{
                      Name            = "heavy-gold"
                      "Environment.$" = "States.Array(States.StringToJson(States.Format('{{\"Name\":\"ROLLUP_DAY\",\"Value\":\"{}\"}}', $.day)))"
                    }]
                  }
                }
                End = true
              }
            }
          },
        ]
        End = true
      }
    }
  })
}

resource "aws_iam_role" "sfn" {
  name               = "rail-archiver-sfn"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "sfn" {
  name = "rail-archiver-sfn"
  role = aws_iam_role.sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "RunStageTasks"
        Effect = "Allow"
        Action = ["ecs:RunTask"]
        Resource = [
          "${aws_ecs_task_definition.rollup.arn_without_revision}:*",
          "${aws_ecs_task_definition.stage["gold"].arn_without_revision}:*",
          "${aws_ecs_task_definition.stage["snapshot"].arn_without_revision}:*",
          "${aws_ecs_task_definition.stage["archive"].arn_without_revision}:*",
          # heavy_stages.tf's third branch (HeavyRollup -> HeavyGold), added
          # 2026-09-05 alongside heavy_stage.
          "${aws_ecs_task_definition.heavy_stage["rollup"].arn_without_revision}:*",
          "${aws_ecs_task_definition.heavy_stage["gold"].arn_without_revision}:*",
        ]
      },
      {
        # runTask.sync needs to be able to stop what it started, and to describe
        # it while polling.
        Sid      = "ManageStartedTasks"
        Effect   = "Allow"
        Action   = ["ecs:StopTask", "ecs:DescribeTasks"]
        Resource = ["*"]
      },
      {
        # The .sync ("run a job") integration polls via a managed EventBridge
        # rule that Step Functions creates on first use. Without these it fails
        # at runtime, not at deploy time -- the classic .sync gotcha.
        Sid    = "SyncIntegrationEventBridge"
        Effect = "Allow"
        Action = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
        Resource = [
          "arn:aws:events:${var.region}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForECSTaskRule"
        ]
      },
      {
        # Handing the task its execution + task roles.
        Sid      = "PassTaskRoles"
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [aws_iam_role.rollup_execution.arn, aws_iam_role.rollup_task.arn]
      },
    ]
  })
}

resource "aws_sfn_state_machine" "nightly" {
  name     = "rail-archiver-nightly"
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"

  definition = local.sfn_definition
}

# The state machine replaces the main rollup task's own schedule once enabled --
# otherwise rollup runs twice. Gated on the same flag as everything else in the
# handover so it cannot drift; see rollup_schedule.tf, whose schedule is
# disabled by the same condition.
resource "aws_scheduler_schedule" "nightly" {
  name  = "rail-archiver-nightly-sfn"
  state = var.stage_schedule_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.stage_schedule_expression
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_sfn_state_machine.nightly.arn
    role_arn = aws_iam_role.sfn_scheduler.arn
    # Empty day => each task's own `${ROLLUP_DAY:-$(date -u -d yesterday +%F)}`
    # picks yesterday, matching the standalone-schedule behaviour.
    input = jsonencode({ day = "" })
  }
}

# Reuses rollup_schedule.tf's scheduler_assume document -- same
# scheduler.amazonaws.com principal, no reason for a second copy.
resource "aws_iam_role" "sfn_scheduler" {
  name               = "rail-archiver-sfn-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

resource "aws_iam_role_policy" "sfn_scheduler" {
  name = "rail-archiver-sfn-scheduler"
  role = aws_iam_role.sfn_scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = [aws_sfn_state_machine.nightly.arn]
    }]
  })
}
