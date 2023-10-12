variable "image_uri" {
  type        = string
  description = "ECR image URI containing the function's deployment package."
}

variable "lambda_timeout" {
  default     = 3
  description = "Amount of time your Lambda Function has to run in seconds. Defaults to 3"
}

variable "subnet_ids" {
  type        = list(string)
  description = "List of subnet IDs associated with the Lambda function."
}

variable "security_group_ids" {
  type        = list(string)
  description = "List of security group IDs associated with the Lambda function"
}

variable "environment" {
  type        = map(string)
  description = "Environment variables to set on lambda, AWS_CONNECTION_STRING_LOCATION automatically set"
  default     = {}
}

variable "function_name" {
  type        = string
  description = "Unique name for Lambda Function"
}

variable "cron_schedule" {
  type        = string
  description = "The CRON scheduling expression only (without wrapping 'cron()')"
}

variable "retention_in_days" {
  default     = 14
  description = "Number of days to retain CloudWatch logs for"
}

variable "ssm_connection_string" {
  type        = string
  description = "SSM key storing connection string"
}