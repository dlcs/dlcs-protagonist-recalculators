# Terraform

Terraform modules for running the 2 recalculator functions.

These are ran as `EC2` tasks on specified schedule.

`scheduled/` is intended as an internal module shared by `customer_storage/` and `entity_counter/`.

There is an optional `cloudwatch_alarm_sns_arn` variable which will create an alarm based on failures in the recalculator