module "entity_counter" {
  source = "../scheduled"

  recalc_type = "entity-counter"

  connection_string_secret = var.connection_string_secret
  cloudwatch_log_group     = var.cloudwatch_log_group
  region                   = var.region
  account_id               = var.account_id
  docker_image             = var.docker_image
  docker_tag               = var.docker_tag
  environment              = var.environment
  prefix                   = var.prefix
  schedule                 = var.schedule
  cluster_arn              = var.cluster_arn
}

