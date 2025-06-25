variable "functions" {
  description = "Map of Lambda functions from FaaSr JSON config"
  type = map(object({
    FunctionName = string
    FaaSServer   = string
    Arguments    = optional(map(any), {})
    InvokeNext   = optional(any, [])
    server_config = object({
      FaaSType = string
      Region   = optional(string)
    })
  }))
}

variable "json_prefix" {
  description = "Prefix derived from JSON filename"
  type        = string
}

variable "lambda_role_arn" {
  description = "ARN of the IAM role for Lambda functions"
  type        = string
}

variable "action_containers" {
  description = "Map of container images from FaaSr JSON config"
  type        = map(string)
}

variable "lambda_timeout" {
  description = "Timeout for Lambda functions in seconds"
  type        = number
  default     = 300
}

variable "lambda_memory_size" {
  description = "Memory size for Lambda functions in MB"
  type        = number
  default     = 256
}

variable "enable_function_urls" {
  description = "Whether to create Lambda function URLs"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
} 