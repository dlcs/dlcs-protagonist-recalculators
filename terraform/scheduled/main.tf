locals {
  secrets = {
    "CONNECTION_STRING" = var.connection_string_secret
  }

  full_name = "${var.prefix}-${var.recalc_type}"
}

module "recalc_container_definition" {
  source = "git::https://github.com/digirati-co-uk/terraform-aws-modules.git//tf/modules/ecs/container_definition/?ref=v3.35"

  secrets     = local.secrets
  environment = var.environment

  log_configuration = {
    logDriver = "awslogs"
    options = {
      "awslogs-group"         = var.cloudwatch_log_group,
      "awslogs-region"        = var.region,
      "awslogs-create-group"  = "true",
      "awslogs-stream-prefix" = local.full_name
    },
    secretOptions = null
  }

  memory = 128

  name  = local.full_name
  image = "${var.docker_image}:${var.docker_tag}"
}

module "recalc_task" {
  source = "git::https://github.com/digirati-co-uk/terraform-aws-modules.git//tf/modules/ecs/task_definition/?ref=v3.35"

  task_name    = local.full_name
  network_mode = "bridge"
  launch_types = ["EC2"]

  container_definitions = [module.recalc_container_definition.container_definition]
}

module "recalc_secrets" {
  source = "git::https://github.com/digirati-co-uk/terraform-aws-modules.git//tf/modules/services/tasks/secrets/?ref=v3.35"

  role_name = module.recalc_task.task_execution_role_name
  secrets   = local.secrets
}

# Permissions
data "aws_iam_policy_document" "put_cloudwatch_metrics" {
  statement {
    effect = "Allow"

    actions = [
      "cloudwatch:PutMetricData",
    ]

    resources = [
      "*"
    ]
  }
}

resource "aws_iam_role_policy" "put_cloudwatch_metrics" {
  name   = "${local.full_name}-put-cloudwatch-metrics"
  role   = module.recalc_task.task_role_name
  policy = data.aws_iam_policy_document.put_cloudwatch_metrics.json
}

# Scheduling..
data "aws_iam_policy_document" "cloudwatch_event_assume_role" {
  statement {
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole",
    ]
  }
}

resource "aws_iam_role_policy_attachment" "task_execution_policy" {
  role       = module.recalc_task.task_role_name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "scheduled_task" {
  name               = "${local.full_name}-scheduled-task"
  assume_role_policy = data.aws_iam_policy_document.cloudwatch_event_assume_role.json
}

data "aws_iam_policy_document" "events_access_ecs" {
  statement {
    effect    = "Allow"
    actions   = ["ecs:RunTask"]
    resources = ["arn:aws:ecs:${var.region}:${var.account_id}:task-definition/${local.full_name}:*"]
  }

  statement {
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = ["*"]

    condition {
      test     = "StringLike"
      variable = "iam:PassedToService"
      values   = ["ecs-tasks.amazonaws.com"]
    }
  }

  statement {
    effect    = "Allow"
    actions   = ["ecs:TagResource"]
    resources = ["*"]

    condition {
      test     = "StringLike"
      variable = "ecs:CreateAction"
      values   = ["RunTask"]
    }
  }
}

resource "aws_iam_role_policy" "events_access_ecs" {
  name   = "${local.full_name}-scheduled-task-events-ecs"
  role   = aws_iam_role.scheduled_task.id
  policy = data.aws_iam_policy_document.events_access_ecs.json
}

resource "aws_cloudwatch_event_rule" "event" {
  name                = local.full_name
  description         = "Runs ${local.full_name} task at a scheduled time"
  schedule_expression = var.schedule
}

resource "aws_cloudwatch_event_target" "event_target" {
  target_id = local.full_name
  rule      = aws_cloudwatch_event_rule.event.name
  arn       = var.cluster_arn
  role_arn  = aws_iam_role.scheduled_task.arn

  ecs_target {
    launch_type         = "EC2"
    task_definition_arn = module.recalc_task.arn
  }
}
