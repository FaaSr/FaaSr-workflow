#!/usr/bin/env python3

import argparse
import json
import os
import sys
import boto3
from github import Github
import base64
import tempfile
import shutil
import subprocess
import requests

def parse_arguments():
    parser = argparse.ArgumentParser(description='Deploy FaaSr functions to specified platform')
    parser.add_argument('--workflow-file', required=True,
                      help='Path to the workflow JSON file')
    return parser.parse_args()

def read_workflow_file(file_path):
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Workflow file {file_path} not found")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in workflow file {file_path}")
        sys.exit(1)

def get_github_token():
    # Get GitHub PAT from environment variable
    token = os.getenv('PAT')
    if not token:
        print("Error: PAT environment variable not set")
        sys.exit(1)
    return token

def get_aws_credentials():
    # Try to get AWS credentials from environment variables
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-west-2')
    role_arn = os.getenv('AWS_LAMBDA_ROLE_ARN')
    
    if not all([aws_access_key, aws_secret_key, role_arn]):
        print("Error: AWS credentials or role ARN not set in environment variables")
        sys.exit(1)
    
    return aws_access_key, aws_secret_key, aws_region, role_arn

def set_github_variable(repo_full_name, var_name, var_value, github_token):
    url = f"https://api.github.com/repos/{repo_full_name}/actions/variables/{var_name}"
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github+json"
    }
    data = {"name": var_name, "value": var_value}
    # Try to update, if not found, create
    r = requests.patch(url, headers=headers, json=data)
    if r.status_code == 404:
        r = requests.post(f"https://api.github.com/repos/{repo_full_name}/actions/variables", headers=headers, json=data)
    if not r.ok:
        print(f"Failed to set variable {var_name}: {r.text}")
    else:
        print(f"Set variable {var_name} for {repo_full_name}")

def ensure_github_secrets_and_vars(repo, required_secrets, required_vars, github_token):
    # Check and set secrets
    existing_secrets = {s.name for s in repo.get_secrets()}
    for secret_name, secret_value in required_secrets.items():
        if secret_name not in existing_secrets:
            print(f"Setting secret: {secret_name}")
        else:
            print(f"Secret {secret_name} already exists, updating it.")
        repo.create_secret(secret_name, secret_value)

    # Set variables using REST API
    for var_name, var_value in required_vars.items():
        set_github_variable(repo.full_name, var_name, var_value, github_token)

def deploy_to_github(workflow_data):
    github_token = get_github_token()
    g = Github(github_token)
    
    for func_name, func_data in workflow_data['FunctionList'].items():
        actual_func_name = func_data['FunctionName']
        repo_name = workflow_data['FunctionGitRepo'][actual_func_name]
        try:
            repo = g.get_repo(repo_name)
            
            # Ensure required secrets and variables are set using environment variables
            required_secrets = {
                "SECRET_PAYLOAD": json.dumps(github_token),
                "PAT": github_token
            }
            required_vars = {
                "PAYLOAD_REPO": f"{repo_name}/payload.json"
            }
            ensure_github_secrets_and_vars(repo, required_secrets, required_vars, github_token)
            
            # First, create/update the workflow JSON file
            workflow_json_path = f"workflows/{func_name}.json"
            try:
                # Try to get the file first
                contents = repo.get_contents(workflow_json_path)
                # If file exists, update it
                repo.update_file(
                    path=workflow_json_path,
                    message=f"Update workflow JSON for {func_name}",
                    content=json.dumps(workflow_data, indent=4),
                    sha=contents.sha,
                    branch="main"
                )
            except Exception as e:
                if "Not Found" in str(e):
                    # If file doesn't exist, create it
                    repo.create_file(
                        path=workflow_json_path,
                        message=f"Add workflow JSON for {func_name}",
                        content=json.dumps(workflow_data, indent=4),
                        branch="main"
                    )
                else:
                    raise e
            
            # Create/update the payload.json file at the root of the repository
            payload_json_path = "payload.json"
            try:
                # Try to get the file first
                contents = repo.get_contents(payload_json_path)
                # If file exists, update it
                repo.update_file(
                    path=payload_json_path,
                    message=f"Update payload.json for {func_name}",
                    content=json.dumps(workflow_data, indent=4),
                    sha=contents.sha,
                    branch="main"
                )
            except Exception as e:
                if "Not Found" in str(e):
                    # If file doesn't exist, create it
                    repo.create_file(
                        path=payload_json_path,
                        message=f"Add payload.json for {func_name}",
                        content=json.dumps(workflow_data, indent=4),
                        branch="main"
                    )
                else:
                    raise e
            
            # Create workflow file
            workflow_content = f"""name: Running Action- {func_name}

on:
  workflow_dispatch:
    inputs:
      PAYLOAD:
        description: 'Payload'
        required: false
jobs:
  run_docker_image:
    runs-on: ubuntu-latest
    container: {workflow_data['ActionContainers'][func_name]}
    env:
      SECRET_PAYLOAD: ${{{{ secrets.SECRET_PAYLOAD }}}}
      GITHUB_PAT: ${{{{ secrets.PAT }}}}
      PAYLOAD_REPO: ${{{{ vars.PAYLOAD_REPO }}}}
      PAYLOAD: ${{{{ github.event.inputs.PAYLOAD }}}}
    steps:
    - name: run Rscript
      run: |
        cd /action
        Rscript faasr_{func_name}_invoke_github-actions.R
"""
            
            # Create or update the workflow file
            workflow_path = f".github/workflows/{func_name}.yml"
            try:
                # Try to get the file first
                contents = repo.get_contents(workflow_path)
                # If file exists, update it
                repo.update_file(
                    path=workflow_path,
                    message=f"Update workflow for {func_name}",
                    content=workflow_content,
                    sha=contents.sha,
                    branch="main"
                )
            except Exception as e:
                if "Not Found" in str(e):
                    # If file doesn't exist, create it
                    repo.create_file(
                        path=workflow_path,
                        message=f"Add workflow for {func_name}",
                        content=workflow_content,
                        branch="main"
                    )
                else:
                    raise e
                    
            print(f"Successfully deployed {actual_func_name} to GitHub")
            
        except Exception as e:
            print(f"Error deploying {actual_func_name} to GitHub: {str(e)}")
            sys.exit(1)

def deploy_to_aws(workflow_data):
    # Get AWS credentials
    aws_access_key, aws_secret_key, aws_region, role_arn = get_aws_credentials()
    
    lambda_client = boto3.client(
        'lambda',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    
    # Process each function in the workflow
    for func_name, func_data in workflow_data['FunctionList'].items():
        try:
            actual_func_name = func_data['FunctionName']
            
            # Create or update Lambda function
            try:
                lambda_client.create_function(
                    FunctionName=actual_func_name,
                    PackageType='Image',
                    Code={'ImageUri': '145342739029.dkr.ecr.us-east-1.amazonaws.com/aws-lambda-tidyverse:latest'},
                    Role=role_arn,
                    Timeout=300,
                    MemorySize=256
                )
            except lambda_client.exceptions.ResourceConflictException:
                # Update existing function
                lambda_client.update_function_code(
                    FunctionName=actual_func_name,
                    ImageUri='145342739029.dkr.ecr.us-east-1.amazonaws.com/aws-lambda-tidyverse:latest'
                )
            
            print(f"Successfully deployed {actual_func_name} to AWS Lambda")
            
        except Exception as e:
            print(f"Error deploying {func_name} to AWS: {str(e)}")
            sys.exit(1)

def main():
    args = parse_arguments()
    workflow_data = read_workflow_file(args.workflow_file)
    
    # Store the workflow file path in the workflow data
    workflow_data['_workflow_file'] = args.workflow_file
    
    # Get FaaSType from workflow data
    faas_type = None
    for server in workflow_data.get('ComputeServers', {}).values():
        if 'FaaSType' in server:
            faas_type = server['FaaSType'].lower()
            break
    
    if faas_type == 'lambda':
        deploy_to_aws(workflow_data)
    elif faas_type == 'githubactions':
        deploy_to_github(workflow_data)
    else:
        print(f"Error: Invalid FaaSType '{faas_type}' in workflow file. Must be 'Lambda' or 'GithubActions'")
        sys.exit(1)

if __name__ == '__main__':
    main() 