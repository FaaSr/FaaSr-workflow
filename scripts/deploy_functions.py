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

def parse_arguments():
    parser = argparse.ArgumentParser(description='Deploy FaaSr functions to specified platform')
    parser.add_argument('--platform', required=True, choices=['aws', 'github'],
                      help='Platform to deploy functions to (aws/github)')
    parser.add_argument('--workflow-file', required=True,
                      help='Path to the workflow JSON file')
    parser.add_argument('--folder', required=True,
                      help='Folder containing R function files')
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


def deploy_to_github(workflow_data):
    github_token = get_github_token()
    g = Github(github_token)
    
    for func_name, func_data in workflow_data['FunctionList'].items():
        repo_name = workflow_data['FunctionGitRepo'][func_name]
        try:
            repo = g.get_repo(repo_name)
            
            # Generate inputs based on function arguments
            inputs_content = "\n".join([
                f"      {arg}:\n"
                f"        description: '{arg} parameter'\n"
                f"        required: true\n"
                f"        type: string"
                for arg in func_data['Arguments'].keys()
            ])
            
            # Create workflow file
            workflow_content = f"""name: {func_name}

on:
  workflow_dispatch:
    inputs:
{inputs_content}

jobs:
  run-function:
    runs-on: ubuntu-latest
    container: {workflow_data['ActionContainers'][func_name]}
    steps:
      - name: Checkout repository
        uses: actions/checkout@v3
        with:
          repository: {repo_name}
"""
            
            # Create or update the workflow file
            try:
                repo.create_file(
                    f".github/workflows/{func_name}.yml",
                    f"Add workflow for {func_name}",
                    workflow_content,
                    branch="main"
                )
            except Exception as e:
                if "already exists" in str(e):
                    # Update existing file
                    contents = repo.get_contents(f".github/workflows/{func_name}.yml")
                    repo.update_file(
                        contents.path,
                        f"Update workflow for {func_name}",
                        workflow_content,
                        contents.sha,
                        branch="main"
                    )
                else:
                    raise e
                    
            print(f"Successfully deployed {func_name} to GitHub")
            
        except Exception as e:
            print(f"Error deploying {func_name} to GitHub: {str(e)}")
            sys.exit(1)

def deploy_to_aws(workflow_data, r_files_folder):
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
    
    if args.platform == 'github':
        deploy_to_github(workflow_data)
    elif args.platform == 'aws':
        deploy_to_aws(workflow_data, args.folder)

if __name__ == '__main__':
    main() 