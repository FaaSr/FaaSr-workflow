terraform {
  required_providers {
    github = {
      source  = "integrations/github"
      version = "~> 5.0"
    }
  }
}

# Create GitHub workflow files based on the JSON configuration
resource "github_repository_file" "workflow_files" {
  for_each = var.functions
  
  repository = var.repository_name
  branch     = "main"
  file       = ".github/workflows/${var.json_prefix}_${each.key}.yml"
  
  content = templatefile("${path.module}/templates/workflow.yml.tpl", {
    workflow_name   = "${var.json_prefix}_${each.key}"
    function_name   = each.key
    container_image = var.action_containers[each.key]
  })
  
  commit_message = "Terraform: Update workflow for ${var.json_prefix}_${each.key}"
  commit_author  = "Terraform FaaSr"
  commit_email   = "terraform@faasr.io"
  
  # Always overwrite existing files
  overwrite_on_create = true
  
  lifecycle {
    # Prevent recreation when content changes slightly
    ignore_changes = [
      commit_author,
      commit_email
    ]
  }
} 