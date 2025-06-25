terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    github = {
      source  = "integrations/github"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

provider "github" {
  token = var.github_token
}

# Read the existing JSON workflow file
locals {
  workflow_file_path = var.workflow_file_path
  workflow_data      = jsondecode(file(local.workflow_file_path))
  json_prefix        = replace(basename(local.workflow_file_path), ".json", "")
  
  # Extract Lambda functions from the JSON config
  lambda_functions = {
    for func_name, func_data in local.workflow_data.FunctionList : func_name => merge(func_data, {
      server_config = local.workflow_data.ComputeServers[func_data.FaaSServer]
    }) if try(local.workflow_data.ComputeServers[func_data.FaaSServer].FaaSType, "") == "Lambda"
  }
  
  # Extract GitHub Actions functions from the JSON config
  github_functions = {
    for func_name, func_data in local.workflow_data.FunctionList : func_name => merge(func_data, {
      server_config = local.workflow_data.ComputeServers[func_data.FaaSServer]
    }) if try(local.workflow_data.ComputeServers[func_data.FaaSServer].FaaSType, "") == "GithubActions"
  }
}

# AWS Lambda Functions
module "aws_lambda" {
  source = "./modules/aws-lambda"
  count  = length(local.lambda_functions) > 0 ? 1 : 0
  
  functions           = local.lambda_functions
  json_prefix        = local.json_prefix
  lambda_role_arn    = var.aws_lambda_role_arn
  action_containers  = local.workflow_data.ActionContainers
  
  tags = var.tags
}

# GitHub Actions Workflows
module "github_workflows" {
  source = "./modules/github-workflows"
  count  = length(local.github_functions) > 0 ? 1 : 0
  
  functions           = local.github_functions
  json_prefix        = local.json_prefix
  repository_name    = var.github_repository
  action_containers  = local.workflow_data.ActionContainers
  
  depends_on = [module.github_secrets]
}

# GitHub Secrets Management
module "github_secrets" {
  source = "./modules/github-secrets"
  count  = length(local.github_functions) > 0 ? 1 : 0
  
  repository_name = var.github_repository
  github_token    = var.github_token
}

# Output information about deployed functions
output "deployed_lambda_functions" {
  description = "List of deployed Lambda functions"
  value = length(local.lambda_functions) > 0 ? [
    for name, func in local.lambda_functions : {
      name     = "${local.json_prefix}_${name}"
      platform = "AWS Lambda"
      container = local.workflow_data.ActionContainers[name]
    }
  ] : []
}

output "deployed_github_workflows" {
  description = "List of deployed GitHub workflows"
  value = length(local.github_functions) > 0 ? [
    for name, func in local.github_functions : {
      name     = "${local.json_prefix}_${name}"
      platform = "GitHub Actions"
      container = local.workflow_data.ActionContainers[name]
    }
  ] : []
}

output "workflow_summary" {
  description = "Summary of the deployed workflow"
  value = {
    workflow_file     = local.workflow_file_path
    json_prefix      = local.json_prefix
    total_functions  = length(local.workflow_data.FunctionList)
    lambda_functions = length(local.lambda_functions)
    github_functions = length(local.github_functions)
    platforms        = distinct([for func in local.workflow_data.FunctionList : local.workflow_data.ComputeServers[func.FaaSServer].FaaSType])
  }
} 