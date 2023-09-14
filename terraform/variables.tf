variable "region" {
}

variable "image_uri" {
}

variable "lambda_timeout" {
}

variable "subnet_ids" {
}

variable "security_group_ids" {
}

variable "environment" {
  type = map(string)
}

variable "function_name" {

}

variable "cron_schedule" {

}

variable "retention_in_days" {
  default = 14
}