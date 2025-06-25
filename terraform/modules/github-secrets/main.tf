terraform {
  required_providers {
    github = {
      source  = "integrations/github"
      version = "~> 5.0"
    }
  }
}

# Manage GitHub repository secrets
resource "github_actions_secret" "secret_payload" {
  repository      = var.repository_name
  secret_name     = "SECRET_PAYLOAD"
  plaintext_value = jsonencode(var.github_token)
}

resource "github_actions_secret" "pat" {
  repository      = var.repository_name
  secret_name     = "PAT"
  plaintext_value = var.github_token
}

# Optional: Additional secrets that might be needed
resource "github_actions_secret" "additional_secrets" {
  for_each = var.additional_secrets
  
  repository      = var.repository_name
  secret_name     = each.key
  plaintext_value = each.value
} 