variable "repository_name" {
  description = "GitHub repository name (format: owner/repo)"
  type        = string
}

variable "github_token" {
  description = "GitHub Personal Access Token"
  type        = string
  sensitive   = true
}

variable "additional_secrets" {
  description = "Additional secrets to create in the repository"
  type        = map(string)
  default     = {}
  sensitive   = true
} 