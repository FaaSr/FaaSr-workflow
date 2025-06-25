variable "functions" {
  description = "Map of GitHub Actions functions from FaaSr JSON config"
  type = map(object({
    FunctionName = string
    FaaSServer   = string
    Arguments    = optional(map(any), {})
    InvokeNext   = optional(any, [])
    server_config = object({
      FaaSType = string
    })
  }))
}

variable "json_prefix" {
  description = "Prefix derived from JSON filename"
  type        = string
}

variable "repository_name" {
  description = "GitHub repository name (format: owner/repo)"
  type        = string
}

variable "action_containers" {
  description = "Map of container images from FaaSr JSON config"
  type        = map(string)
} 