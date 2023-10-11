resource "aws_cloudwatch_log_group" "lambda_docker_log_group" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.retention_in_days
}

resource "aws_lambda_function" "lambda_docker_function" {
  function_name = var.function_name
  package_type  = "Image"
  role          = aws_iam_role.lambda_docker_function_exec_role.arn
  image_uri     = var.image_uri
  timeout       = var.lambda_timeout

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  environment {
    variables = var.environment
  }
}

data "aws_iam_policy_document" "lambda_function_exec_role" {
  statement {
    actions = [
      "sts:AssumeRole",
    ]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_docker_function_exec_role" {
  name               = "${var.function_name}-exec-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_function_exec_role.json
}

resource "aws_iam_role_policy" "lambda_docker_function_logging" {
  name   = "${var.function_name}-exec-role"
  role   = aws_iam_role.lambda_docker_function_exec_role.name
  policy = data.aws_iam_policy_document.lambda_docker_function_permissions.json
}

data "aws_iam_policy_document" "lambda_docker_function_permissions" {
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "ec2:CreateNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DeleteNetworkInterface",
      "ec2:AssignPrivateIpAddresses",
      "ec2:UnassignPrivateIpAddresses",
      "ssm:GetParameter",
      "cloudwatch:PutMetricData"
    ]

    resources = ["*"]
  }
}

resource "aws_cloudwatch_event_rule" "lambda_docker_cloudwatch_cron_rule" {
  name                = var.function_name
  description         = "${var.function_name} CRON rule for scheduling runs"
  schedule_expression = "cron(${var.cron_schedule})"
}

resource "aws_lambda_permission" "lambda_docker_cron_permission" {
  statement_id  = "${var.function_name}-allow-cloudwatch-execution"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_docker_function.arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.lambda_docker_cloudwatch_cron_rule.arn
}

resource "aws_cloudwatch_event_target" "lambda_docker_cloudwatch_target" {
  rule = aws_cloudwatch_event_rule.lambda_docker_cloudwatch_cron_rule.name
  arn  = aws_lambda_function.lambda_docker_function.arn
}
