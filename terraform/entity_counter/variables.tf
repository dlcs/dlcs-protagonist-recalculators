variable "connection_string_secret" {
  type        = string
  description = "Path to SecretsManager secret containing connection string"
}

variable "cloudwatch_log_group" {
  type        = string
  description = "CW Log group for logs"
}

variable "region" {
  type        = string
  description = "AWS region"
}

variable "account_id" {
  type        = string
  description = "AWS account id"
}

variable "docker_image" {
  type    = string
  default = "ghcr.io/dlcs/entity-counter-recalculator"
}

variable "docker_tag" {
  type    = string
  default = "latest"
}

variable "environment" {
  type        = map(string)
  description = "Environment variables to set on ECS task"
  default     = {}
}

variable "prefix" {
  type        = string
  description = "Prefix value for use in naming"
}

variable "schedule" {
  type        = string
  description = "Scheduling expressing, either cron() or rate() expression"
}

variable "cluster_arn" {
  type        = string
  description = "ARN of ECS cluster for task"
}
