output "function_name" {
  value = aws_lambda_function.lambda_docker_function.function_name
}

output "function_arn" {
  value = aws_lambda_function.lambda_docker_function.arn
}

output "function_role_arn" {
  value = aws_iam_role.lambda_docker_function_exec_role.arn
}
