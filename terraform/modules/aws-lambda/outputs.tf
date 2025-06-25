output "lambda_functions" {
  description = "Map of created Lambda functions"
  value = {
    for k, v in aws_lambda_function.faasr_functions : k => {
      arn           = v.arn
      function_name = v.function_name
      invoke_arn    = v.invoke_arn
      image_uri     = v.image_uri
      last_modified = v.last_modified
    }
  }
}

output "function_urls" {
  description = "Map of Lambda function URLs (if enabled)"
  value = {
    for k, v in aws_lambda_function_url.faasr_function_urls : k => {
      function_url = v.function_url
      url_id       = v.url_id
    }
  }
}

output "function_summary" {
  description = "Summary of deployed Lambda functions"
  value = {
    count = length(aws_lambda_function.faasr_functions)
    names = [for f in aws_lambda_function.faasr_functions : f.function_name]
  }
} 