# Terraform

Terraform modules for running the 2 recalculator functions.

These are ran as `FARGATE_SPOT` tasks on specified schedule.

`scheduled/` is intended as an internal module shared by `customer_storage/` and `entity_counter/`.