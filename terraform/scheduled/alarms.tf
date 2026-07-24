resource "aws_cloudwatch_event_rule" "recalculator_task_failed" {
  name = "${local.full_name}-task-failed"

  event_pattern = jsonencode({
    source      = ["aws.ecs"]
    detail-type = ["ECS Task State Change"]
    detail = {
      clusterArn = [var.cluster_arn]
      lastStatus = ["STOPPED"]
      taskDefinitionArn = [
        { prefix = "arn:aws:ecs:${var.region}:${var.account_id}:task-definition/${local.full_name}:" }
      ]
      "$or" = [
        { containers = { exitCode = [{ "anything-but" = 0 }] } },
        { stopCode = ["TaskFailedToStart"] }
      ]
    }
  })
}

resource "aws_cloudwatch_log_group" "recalculator_task_failures" {
  name              = "/aws/events/${local.full_name}-task-failures"
  retention_in_days = 14
}

data "aws_iam_policy_document" "recalc_task_failure_events_log_policy" {
  statement {
    effect  = "Allow"
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = [
      "${aws_cloudwatch_log_group.recalculator_task_failures.arn}:*",
    ]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_cloudwatch_log_resource_policy" "recalculator_task_failure_events" {
  policy_name     = "${local.full_name}-recalculator-task-failure-events"
  policy_document = data.aws_iam_policy_document.recalc_task_failure_events_log_policy.json
}

resource "aws_cloudwatch_event_target" "recalculator_task_failed" {
  rule = aws_cloudwatch_event_rule.recalculator_task_failed.name
  arn  = aws_cloudwatch_log_group.recalculator_task_failures.arn

  depends_on = [aws_cloudwatch_log_resource_policy.recalculator_task_failure_events]
}

resource "aws_cloudwatch_log_metric_filter" "recalculator_task_failed" {
  name           = "${local.full_name}-task-failed"
  log_group_name = aws_cloudwatch_log_group.recalculator_task_failures.name
  pattern        = ""

  metric_transformation {
    name          = "${local.recalc_type_pascal}RecalculatorTaskFailures"
    namespace     = "${var.prefix}/Recalculators"
    value         = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "recalculator_error" {
  count = var.cloudwatch_alarm_sns_arn != null ? 1 : 0

  alarm_name         = "${local.full_name}_recalculator_error"
  alarm_description  = "${local.full_name} recalculator ECS task exited with a non-zero status"
  evaluation_periods = 1
  period             = 300

  comparison_operator = "GreaterThanOrEqualToThreshold"
  metric_name         = aws_cloudwatch_log_metric_filter.recalculator_task_failed.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.recalculator_task_failed.metric_transformation[0].namespace
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  alarm_actions = [var.cloudwatch_alarm_sns_arn]
}