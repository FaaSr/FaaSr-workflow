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
    aws_region = 'us-east-1'
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
    
    # Get the current repository name from the workflow file path
    workflow_file = workflow_data['_workflow_file']
    json_prefix = os.path.splitext(os.path.basename(workflow_file))[0]
    
    # Get the current repository
    repo_name = os.getenv('GITHUB_REPOSITORY')
    if not repo_name:
        print("Error: GITHUB_REPOSITORY environment variable not set")
        sys.exit(1)
    
    # Filter functions that should be deployed to GitHub Actions
    github_functions = {}
    for func_name, func_data in workflow_data['FunctionList'].items():
        server_name = func_data['FaaSServer']
        server_config = workflow_data['ComputeServers'][server_name]
        if server_config['FaaSType'].lower() == 'githubactions':
            github_functions[func_name] = func_data
    
    if not github_functions:
        print("No functions found for GitHub Actions deployment")
        return
    
    try:
        repo = g.get_repo(repo_name)
        
        # Ensure required secrets and variables are set using environment variables
        required_secrets = {
            "SECRET_PAYLOAD": json.dumps(github_token),
            "PAT": github_token
        }
        ensure_github_secrets_and_vars(repo, required_secrets, {}, github_token)
        
        for func_name, func_data in github_functions.items():
            actual_func_name = func_data['FunctionName']
            
            # Create workflow file with prefixed name
            workflow_content = f"""name: {json_prefix}_{func_name}

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
      PAYLOAD: ${{{{ github.event.inputs.PAYLOAD }}}}
    steps:
    - name: run Rscript
      run: |
        cd /action
        Rscript faasr_{func_name}_invoke_github-actions.R
"""
            
            # Create or update the workflow file
            workflow_path = f".github/workflows/{json_prefix}_{func_name}.yml"
            try:
                # Try to get the file first
                contents = repo.get_contents(workflow_path)
                # If file exists, update it
                repo.update_file(
                    path=workflow_path,
                    message=f"Update workflow for {json_prefix}_{func_name}",
                    content=workflow_content,
                    sha=contents.sha,
                    branch="main"
                )
            except Exception as e:
                if "Not Found" in str(e):
                    # If file doesn't exist, create it
                    repo.create_file(
                        path=workflow_path,
                        message=f"Add workflow for {json_prefix}_{func_name}",
                        content=workflow_content,
                        branch="main"
                    )
                else:
                    raise e
                    
            print(f"Successfully deployed {actual_func_name} to GitHub")
            
    except Exception as e:
        print(f"Error deploying to GitHub: {str(e)}")
        sys.exit(1)

def deploy_to_aws(workflow_data):
    # Get AWS credentials
    aws_access_key, aws_secret_key, aws_region, role_arn = get_aws_credentials()
    
    # Get the JSON file prefix
    workflow_file = workflow_data['_workflow_file']
    json_prefix = os.path.splitext(os.path.basename(workflow_file))[0]
    
    lambda_client = boto3.client(
        'lambda',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    
    # Filter functions that should be deployed to AWS Lambda
    lambda_functions = {}
    for func_name, func_data in workflow_data['FunctionList'].items():
        server_name = func_data['FaaSServer']
        server_config = workflow_data['ComputeServers'][server_name]
        if server_config['FaaSType'].lower() == 'lambda':
            lambda_functions[func_name] = func_data
    
    if not lambda_functions:
        print("No functions found for AWS Lambda deployment")
        return
    
    # Process each function in the workflow
    for func_name, func_data in lambda_functions.items():
        try:
            actual_func_name = func_data['FunctionName']
            # Create prefixed function name
            prefixed_func_name = f"{json_prefix}_{func_name}"
            
            # Create or update Lambda function
            try:
                lambda_client.create_function(
                    FunctionName=prefixed_func_name,
                    PackageType='Image',
                    Code={'ImageUri': '145342739029.dkr.ecr.us-east-1.amazonaws.com/aws-lambda-tidyverse:latest'},
                    Role=role_arn,
                    Timeout=300,
                    MemorySize=256
                )
            except lambda_client.exceptions.ResourceConflictException:
                # Update existing function
                lambda_client.update_function_code(
                    FunctionName=prefixed_func_name,
                    ImageUri='145342739029.dkr.ecr.us-east-1.amazonaws.com/aws-lambda-tidyverse:latest'
                )
            
            print(f"Successfully deployed {prefixed_func_name} to AWS Lambda")
            
        except Exception as e:
            print(f"Error deploying {prefixed_func_name} to AWS: {str(e)}")
            sys.exit(1)


def get_openwhisk_credentials(workflow_data):
    # Get OpenWhisk server configuration from workflow data
    for server_name, server_config in workflow_data['ComputeServers'].items():
        if server_config['FaaSType'].lower() == 'openwhisk':
            return (
                server_config['Endpoint'],
                server_config['Namespace'],
                server_config['SSL'].lower() == 'true'
            )
    
    print("Error: No OpenWhisk server configuration found in workflow data")
    sys.exit(1)

def deploy_to_ow(workflow_data):
    # Get OpenWhisk credentials
    api_host, namespace, ssl = get_openwhisk_credentials(workflow_data)
    
    # Get the JSON file prefix
    workflow_file = workflow_data['_workflow_file']
    json_prefix = os.path.splitext(os.path.basename(workflow_file))[0]
    
    # Filter functions that should be deployed to OpenWhisk
    ow_functions = {}
    for func_name, func_data in workflow_data['FunctionList'].items():
        server_name = func_data['FaaSServer']
        server_config = workflow_data['ComputeServers'][server_name]
        if server_config['FaaSType'].lower() == 'openwhisk':
            ow_functions[func_name] = func_data
    
    if not ow_functions:
        print("No functions found for OpenWhisk deployment")
        return
    
    # Set up wsk properties
    subprocess.run(f"wsk property set --apihost {api_host}", shell=True)
    # Skip auth setting for OpenWhisk without authentication
    print("Using OpenWhisk without authentication")
    # Always use insecure flag to bypass certificate issues
    subprocess.run("wsk property set --insecure", shell=True)
    
    # Set environment variable to handle certificate issue
    env = os.environ.copy()
    env['GODEBUG'] = 'x509ignoreCN=0'
    
    # Process each function in the workflow
    for func_name, func_data in ow_functions.items():
        try:
            actual_func_name = func_data['FunctionName']
            # Create prefixed function name
            prefixed_func_name = f"{json_prefix}_{func_name}"
            
            # Create or update OpenWhisk action using wsk CLI
            try:
                # First check if action exists (add --insecure flag)
                check_cmd = f"wsk action get {prefixed_func_name} --insecure >/dev/null 2>&1"
                exists = subprocess.run(check_cmd, shell=True, env=env).returncode == 0
                
                if exists:
                    # Update existing action (add --insecure flag)
                    cmd = f"wsk action update {prefixed_func_name} --docker {workflow_data['ActionContainers'][func_name]} --insecure"
                else:
                    # Create new action (add --insecure flag)
                    cmd = f"wsk action create {prefixed_func_name} --docker {workflow_data['ActionContainers'][func_name]} --insecure"
                
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)
                
                if result.returncode != 0:
                    raise Exception(f"Failed to {'update' if exists else 'create'} action: {result.stderr}")
                
                print(f"Successfully deployed {prefixed_func_name} to OpenWhisk")
                
            except Exception as e:
                print(f"Error deploying {prefixed_func_name} to OpenWhisk: {str(e)}")
                sys.exit(1)
                
        except Exception as e:
            print(f"Error processing {func_name}: {str(e)}")
            sys.exit(1)

def main():
    args = parse_arguments()
    workflow_data = read_workflow_file(args.workflow_file)
    
    # Store the workflow file path in the workflow data
    workflow_data['_workflow_file'] = args.workflow_file
    
    # Get all unique FaaSTypes from workflow data
    faas_types = set()
    for server in workflow_data.get('ComputeServers', {}).values():
        if 'FaaSType' in server:
            faas_types.add(server['FaaSType'].lower())
    
    if not faas_types:
        print("Error: No FaaSType found in workflow file")
        sys.exit(1)
    
    print(f"Found FaaS platforms: {', '.join(faas_types)}")
    
    # Deploy to each platform found
    for faas_type in faas_types:
        print(f"\nDeploying to {faas_type}...")
        if faas_type == 'lambda':
            deploy_to_aws(workflow_data)
        elif faas_type == 'githubactions':
            deploy_to_github(workflow_data)
        elif faas_type == 'openwhisk':
            deploy_to_ow(workflow_data)
        else:
            print(f"Warning: Unknown FaaSType '{faas_type}' - skipping")
    
    print("\nMulti-platform deployment completed!")

if __name__ == '__main__':
    main() 