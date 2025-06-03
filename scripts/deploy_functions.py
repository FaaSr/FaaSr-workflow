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
    token = os.getenv('GITHUB_PAT')
    if not token:
        print("Error: GITHUB_PAT environment variable not set")
        sys.exit(1)
    return token

def get_aws_credentials():
    # Try to get AWS credentials from environment variables
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-west-2')
    
    if not all([aws_access_key, aws_secret_key]):
        print("Error: AWS credentials not set in environment variables")
        sys.exit(1)
    
    return aws_access_key, aws_secret_key, aws_region

def create_r_lambda_layer(lambda_client, layer_name, r_version="4.2.0"):
    """
    Create a Lambda layer with R runtime
    """
    # Create a temporary directory for building the layer
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create layer structure
        layer_dir = os.path.join(temp_dir, "r")
        os.makedirs(layer_dir, exist_ok=True)
        
        # Create Dockerfile for building R
        dockerfile_content = f"""
        FROM amazonlinux:2
        RUN yum update -y && yum install -y gcc gcc-c++ make wget tar
        WORKDIR /tmp
        RUN wget https://cran.r-project.org/src/base/R-4/R-{r_version}.tar.gz && \
            tar xzf R-{r_version}.tar.gz && \
            cd R-{r_version} && \
            ./configure --prefix=/opt/R && \
            make && make install
        """
        
        with open(os.path.join(temp_dir, "Dockerfile"), "w") as f:
            f.write(dockerfile_content)
        
        # Build R in Docker
        subprocess.run(["docker", "build", "-t", "r-builder", temp_dir])
        
        # Extract R installation
        subprocess.run(["docker", "run", "--rm", "-v", f"{layer_dir}:/output", "r-builder", 
                       "cp", "-r", "/opt/R", "/output/"])
        
        # Create layer zip
        layer_zip = os.path.join(temp_dir, "layer.zip")
        shutil.make_archive(layer_zip[:-4], 'zip', layer_dir)
        
        # Upload layer to Lambda
        with open(layer_zip, 'rb') as f:
            response = lambda_client.publish_layer_version(
                LayerName=layer_name,
                Description=f"R {r_version} runtime for Lambda",
                Content={'ZipFile': f.read()},
                CompatibleRuntimes=['provided.al2']
            )
            return response['LayerVersionArn']

def deploy_to_github(workflow_data):
    # Get GitHub token from environment variable
    github_token = get_github_token()

    g = Github(github_token)
    
    # Process each function in the workflow
    for func_name, func_data in workflow_data['FunctionList'].items():
        repo_name = workflow_data['FunctionGitRepo'][func_name]
        try:
            repo = g.get_repo(repo_name)
            
            # Create or update workflow file for the function
            workflow_content = f"""name: {func_name}

on:
  workflow_dispatch:
    inputs:
      folder:
        description: 'Input folder'
        required: true
        type: string
      input1:
        description: 'First input file'
        required: true
        type: string
      input2:
        description: 'Second input file'
        required: true
        type: string
      output:
        description: 'Output file'
        required: true
        type: string

jobs:
  run-function:
    runs-on: ubuntu-latest
    container: {workflow_data['ActionContainers'][func_name]}
    steps:
      - name: Checkout repository
        uses: actions/checkout@v3
      
      - name: Run R function
        run: |
          Rscript functions/{func_name}.R ${{{{ github.event.inputs.folder }}}} ${{{{ github.event.inputs.input1 }}}} ${{{{ github.event.inputs.input2 }}}} ${{{{ github.event.inputs.output }}}}
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

def deploy_to_aws(workflow_data):
    # Get AWS credentials
    aws_access_key, aws_secret_key, aws_region = get_aws_credentials()
    
    lambda_client = boto3.client(
        'lambda',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    
    # Create R runtime layer
    layer_arn = create_r_lambda_layer(lambda_client, "r-runtime")
    
    # Process each function in the workflow
    for func_name, func_data in workflow_data['FunctionList'].items():
        try:
            # Create a temporary directory for the function package
            with tempfile.TemporaryDirectory() as temp_dir:
                # Copy R function
                shutil.copy(f"functions/{func_name}.R", os.path.join(temp_dir, "index.R"))
                
                # Create bootstrap script
                bootstrap_content = """#!/bin/bash
/opt/r/bin/Rscript /var/task/index.R "$@"
"""
                with open(os.path.join(temp_dir, "bootstrap"), "w") as f:
                    f.write(bootstrap_content)
                os.chmod(os.path.join(temp_dir, "bootstrap"), 0o755)
                
                # Create function zip
                function_zip = os.path.join(temp_dir, "function.zip")
                shutil.make_archive(function_zip[:-4], 'zip', temp_dir)
                
                # Create Lambda function
                with open(function_zip, 'rb') as f:
                    response = lambda_client.create_function(
                        FunctionName=func_name,
                        Runtime='provided.al2',
                        Role='arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-role',  # Replace with your role ARN
                        Handler='bootstrap',
                        Code={'ZipFile': f.read()},
                        Timeout=300,
                        MemorySize=256,
                        Layers=[layer_arn]
                    )
                
                print(f"Successfully deployed {func_name} to AWS Lambda")
                
        except Exception as e:
            print(f"Error deploying {func_name} to AWS: {str(e)}")
            sys.exit(1)

def main():
    args = parse_arguments()
    workflow_data = read_workflow_file(args.workflow_file)
    
    if args.platform == 'github':
        deploy_to_github(workflow_data)
    elif args.platform == 'aws':
        deploy_to_aws(workflow_data)

if __name__ == '__main__':
    main() 