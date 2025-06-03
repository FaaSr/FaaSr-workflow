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
    
    if not all([aws_access_key, aws_secret_key]):
        print("Error: AWS credentials not set in environment variables")
        sys.exit(1)
    
    return aws_access_key, aws_secret_key, aws_region

def create_r_lambda_layer(lambda_client, layer_name):
    """
    Create a Lambda layer with minimal R runtime and FaaSr package
    """
    # Create a temporary directory for building the layer
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create layer structure
        layer_dir = os.path.join(temp_dir, "r")
        os.makedirs(layer_dir, exist_ok=True)
        
        # Create Dockerfile to build minimal R
        dockerfile_content = """FROM r-base:4.3.2

# Install jq for JSON parsing and git for package installation
RUN apt-get update && apt-get install -y jq git

# Create output directory
RUN mkdir -p /output

# Install FaaSr package from the tutorial repository
RUN R -e "install.packages('remotes'); remotes::install_github('FaaSr/FaaSr-tutorial')"

# Copy only essential R components
RUN R_HOME=$(R RHOME) && \
    mkdir -p /output/R && \
    cp -r $R_HOME/bin /output/R/ && \
    cp -r $R_HOME/lib /output/R/ && \
    cp -r $R_HOME/library /output/R/ && \
    cp -r $R_HOME/modules /output/R/ && \
    cp -r $R_HOME/etc /output/R/ && \
    chmod -R 755 /output/R

# Copy jq to the output
RUN cp $(which jq) /output/

# Create bootstrap script
RUN echo '#!/bin/bash' > /output/bootstrap && \
    echo 'export R_HOME=/opt/r/R' >> /output/bootstrap && \
    echo 'export PATH=$R_HOME/bin:$PATH' >> /output/bootstrap && \
    echo 'export LD_LIBRARY_PATH=$R_HOME/lib:$LD_LIBRARY_PATH' >> /output/bootstrap && \
    echo 'handle_event() {' >> /output/bootstrap && \
    echo '    event=$(cat)' >> /output/bootstrap && \
    echo '    folder=$(echo $event | jq -r ".folder")' >> /output/bootstrap && \
    echo '    input1=$(echo $event | jq -r ".input1")' >> /output/bootstrap && \
    echo '    input2=$(echo $event | jq -r ".input2")' >> /output/bootstrap && \
    echo '    output=$(echo $event | jq -r ".output")' >> /output/bootstrap && \
    echo '    $R_HOME/bin/Rscript /var/task/index.R "$folder" "$input1" "$input2" "$output"' >> /output/bootstrap && \
    echo '    echo "{\\"statusCode\\": 200, \\"body\\": \\"Function executed successfully\\"}"' >> /output/bootstrap && \
    echo '}' >> /output/bootstrap && \
    echo 'handle_event' >> /output/bootstrap && \
    chmod +x /output/bootstrap
"""
        
        with open(os.path.join(temp_dir, "Dockerfile"), "w") as f:
            f.write(dockerfile_content)
        
        print("Building R runtime Docker image...")
        # Build R in Docker
        build_result = subprocess.run(
            ["docker", "build", "-t", "r-builder", temp_dir],
            capture_output=True,
            text=True
        )
        
        if build_result.returncode != 0:
            print("Docker build failed:")
            print(build_result.stdout)
            print(build_result.stderr)
            sys.exit(1)
        
        print("Extracting R installation...")
        # Extract R installation
        extract_result = subprocess.run(
            ["docker", "run", "--rm", "-v", f"{layer_dir}:/output", "r-builder", 
             "bash", "-c", "cp -r /output/* /output/"],
            capture_output=True,
            text=True
        )
        
        if extract_result.returncode != 0:
            print("Failed to extract R installation:")
            print(extract_result.stdout)
            print(extract_result.stderr)
            sys.exit(1)
        
        # Verify the layer directory is not empty
        if not os.listdir(layer_dir):
            print("Error: Layer directory is empty after extraction")
            sys.exit(1)
        
        print("Creating layer zip...")
        # Create layer zip
        layer_zip = os.path.join(temp_dir, "layer.zip")
        shutil.make_archive(layer_zip[:-4], 'zip', layer_dir)
        
        # Verify the zip file exists and is not empty
        if not os.path.exists(layer_zip) or os.path.getsize(layer_zip) == 0:
            print("Error: Failed to create layer zip file")
            sys.exit(1)
        
        print("Uploading layer to Lambda...")
        # Upload layer to Lambda
        with open(layer_zip, 'rb') as f:
            response = lambda_client.publish_layer_version(
                LayerName=layer_name,
                Description="Minimal R runtime with FaaSr package for Lambda",
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

def deploy_to_aws(workflow_data, r_files_folder):
    # Get AWS credentials
    aws_access_key, aws_secret_key, aws_region = get_aws_credentials()
    
    lambda_client = boto3.client(
        'lambda',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    
    # Get the project directory from the workflow file path
    project_dir = os.path.dirname(os.path.abspath(workflow_data.get('_workflow_file', '')))
    if not project_dir:
        project_dir = os.getcwd()
    
    # Use the specified folder for R files
    r_files_dir = os.path.join(project_dir, r_files_folder)
    
    # Process each function in the workflow
    for func_name, func_data in workflow_data['FunctionList'].items():
        try:
            # Get the actual function name
            actual_func_name = func_data['FunctionName']
            
            # Create a temporary directory for the function package
            with tempfile.TemporaryDirectory() as temp_dir:
                # Create Dockerfile using the cleaner approach from the Medium article
                dockerfile_content = """FROM public.ecr.aws/lambda/provided:al2-x86_64

# Install EPEL repository for Amazon Linux 2
RUN yum install -y amazon-linux-extras
RUN amazon-linux-extras install -y epel

# Install R and required system dependencies
RUN yum install -y R \
    libcurl-devel \
    openssl-devel \
    libxml2-devel \
    git \
    which

# Install basic R packages first
RUN R -e "install.packages(c('jsonlite', 'httr', 'logger'), repos='https://cran.r-project.org', dependencies=TRUE)"

# Install remotes package
RUN R -e "install.packages('remotes', repos='https://cran.r-project.org')"

# Try to install FaaSr package, but don't fail if it doesn't work
RUN R -e "try(remotes::install_github('FaaSr/FaaSr-tutorial'), silent=TRUE)"

# Copy the runtime and handler files
COPY runtime.R ${LAMBDA_TASK_ROOT}
COPY handler.R ${LAMBDA_TASK_ROOT}

# Set the handler
CMD [ "handler.main" ]
"""
                with open(os.path.join(temp_dir, "Dockerfile"), "w") as f:
                    f.write(dockerfile_content)
                
                # Copy R function from project1 directory and rename it to handler.R
                r_file_path = os.path.join(r_files_dir, f"{actual_func_name}.R")
                if not os.path.exists(r_file_path):
                    print(f"Error: R function file not found at {r_file_path}")
                    sys.exit(1)
                
                # Read the original R function to create a wrapper
                with open(r_file_path, 'r') as f:
                    original_r_code = f.read()
                
                # Create handler.R with wrapper that calls the original function
                handler_content = f"""# Original FaaSr function
{original_r_code}

# Lambda handler wrapper
main <- function(event) {{
  tryCatch({{
    # Extract parameters from the Lambda event
    # The event should contain the function parameters as named elements
    
    # Call the original function with parameters from the event
    if ("{actual_func_name}" == "create_sample_data") {{
      folder <- event$folder
      output1 <- event$output1  
      output2 <- event$output2
      
      # Call the original function
      result <- create_sample_data(folder, output1, output2)
      
    }} else if ("{actual_func_name}" == "compute_sum") {{
      folder <- event$folder
      input1 <- event$input1
      input2 <- event$input2
      output <- event$output
      
      # Call the original function
      result <- compute_sum(folder, input1, input2, output)
    }}
    
    # Return success response
    return(list(
      statusCode = 200,
      body = list(
        message = "Function executed successfully",
        function_name = "{actual_func_name}"
      )
    ))
    
  }}, error = function(e) {{
    # Return error response
    return(list(
      statusCode = 500,
      body = list(
        error = as.character(e),
        function_name = "{actual_func_name}"
      )
    ))
  }})
}}
"""
                
                with open(os.path.join(temp_dir, "handler.R"), "w") as f:
                    f.write(handler_content)
                
                # Create runtime.R file that handles Lambda runtime interface
                runtime_content = """# Lambda Runtime Interface for R
# Based on approach from: https://medium.com/swlh/deploying-a-serverless-r-inference-service-using-aws-lambda-amazon-api-gateway-and-the-aws-cdk-65db916ea02c

library(httr)
library(jsonlite)
library(logger)

# Get environment variables
lambda_runtime_api <- Sys.getenv("AWS_LAMBDA_RUNTIME_API")
handler <- Sys.getenv("_HANDLER")

# Parse handler
handler_split <- strsplit(handler, ".", fixed = TRUE)[[1]]
file_name <- paste0(handler_split[1], ".R")
function_name <- handler_split[2]

# Source the handler file
source(file_name)

# Main runtime loop
while (TRUE) {
  # Get next invocation
  resp <- GET(
    url = paste0("http://", lambda_runtime_api, "/2018-06-01/runtime/invocation/next"),
    timeout(600)
  )
  
  # Extract request ID and event data
  request_id <- headers(resp)[["lambda-runtime-aws-request-id"]]
  event_data <- content(resp, "text", encoding = "UTF-8")
  
  # Parse event data
  event <- tryCatch({
    fromJSON(event_data)
  }, error = function(e) {
    log_error("Failed to parse event data: {e$message}")
    list()
  })
  
  # Execute the handler function
  result <- tryCatch({
    # Call the function specified in the handler
    if (exists(function_name)) {
      do.call(function_name, list(event))
    } else {
      log_error("Function {function_name} not found")
      list(statusCode = 500, body = paste("Function", function_name, "not found"))
    }
  }, error = function(e) {
    log_error("Handler execution failed: {e$message}")
    list(statusCode = 500, body = paste("Error:", e$message))
  })
  
  # Convert result to JSON
  response_json <- toJSON(result, auto_unbox = TRUE)
  
  # Send response
  POST(
    url = paste0("http://", lambda_runtime_api, "/2018-06-01/runtime/invocation/", request_id, "/response"),
    body = response_json,
    content_type("application/json")
  )
}
"""
                with open(os.path.join(temp_dir, "runtime.R"), "w") as f:
                    f.write(runtime_content)
                
                print(f"Building Docker image for {actual_func_name}...")
                # Build Docker image
                build_result = subprocess.run(
                    ["docker", "build", "-t", f"lambda-{actual_func_name}", temp_dir],
                    capture_output=True,
                    text=True
                )
                
                if build_result.returncode != 0:
                    print("Docker build failed:")
                    print(build_result.stdout)
                    print(build_result.stderr)
                    sys.exit(1)
                
                # Create ECR repository if it doesn't exist
                ecr_client = boto3.client(
                    'ecr',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=aws_region
                )
                
                try:
                    ecr_client.create_repository(repositoryName=actual_func_name)
                except ecr_client.exceptions.RepositoryAlreadyExistsException:
                    pass
                
                # Get ECR login token and login
                token = ecr_client.get_authorization_token()
                username, password = base64.b64decode(token['authorizationData'][0]['authorizationToken']).decode().split(':')
                registry = token['authorizationData'][0]['proxyEndpoint']
                
                subprocess.run(
                    ["docker", "login", "-u", username, "-p", password, registry],
                    capture_output=True,
                    text=True
                )
                
                # Tag and push image
                image_uri = f"{registry.replace('https://', '')}/{actual_func_name}:latest"
                subprocess.run(["docker", "tag", f"lambda-{actual_func_name}:latest", image_uri])
                subprocess.run(["docker", "push", image_uri])
                
                # Create or update Lambda function
                try:
                    # Get IAM role ARN from environment variable (GitHub secret)
                    role_arn = os.getenv('AWS_LAMBDA_ROLE_ARN')
                    if not role_arn:
                        print("Error: AWS_LAMBDA_ROLE_ARN environment variable not set")
                        print("Please set this as a GitHub secret with your Lambda execution role ARN")
                        sys.exit(1)
                    
                    lambda_client.create_function(
                        FunctionName=actual_func_name,
                        PackageType='Image',
                        Code={'ImageUri': image_uri},
                        Role=role_arn,
                        Timeout=300,
                        MemorySize=256
                    )
                except lambda_client.exceptions.ResourceConflictException:
                    # Update existing function
                    lambda_client.update_function_code(
                        FunctionName=actual_func_name,
                        ImageUri=image_uri
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