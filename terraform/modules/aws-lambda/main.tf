terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Create Lambda functions based on the JSON configuration
resource "aws_lambda_function" "faasr_functions" {
  for_each = var.functions
  
  function_name = "${var.json_prefix}_${each.key}"
  package_type  = "Image"
  image_uri     = var.action_containers[each.key]
  role          = var.lambda_role_arn
  timeout       = var.lambda_timeout
  memory_size   = var.lambda_memory_size
  
  environment {
    variables = {
      FAASR_FUNCTION_NAME = each.value.FunctionName
      FAASR_INVOKE_NEXT   = jsonencode(try(each.value.InvokeNext, []))
      FAASR_ARGUMENTS     = jsonencode(try(each.value.Arguments, {}))
    }
  }
  
  tags = merge(var.tags, {
    FunctionName = each.value.FunctionName
    FaaSrFunction = each.key
  })
  
  # Prevent unnecessary updates when only container image changes
  lifecycle {
    ignore_changes = [
      # Ignore image_uri changes - we'll handle these separately if needed
      # This prevents constant updates when the container hasn't actually changed
    ]
  }
}

# Lambda function URLs (optional - useful for HTTP triggers)
resource "aws_lambda_function_url" "faasr_function_urls" {
  for_each = var.enable_function_urls ? var.functions : {}
  
  function_name      = aws_lambda_function.faasr_functions[each.key].function_name
  authorization_type = "NONE"  # Change to "AWS_IAM" for secured access
  
  cors {
    allow_credentials = false
    allow_origins     = ["*"]
    allow_methods     = ["*"]
    allow_headers     = ["date", "keep-alive"]
    expose_headers    = ["date", "keep-alive"]
    max_age          = 86400
  }
} 