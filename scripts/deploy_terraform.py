#!/usr/bin/env python3

import argparse
import json
import os
import sys
import subprocess
import tempfile
from pathlib import Path

def parse_arguments():
    parser = argparse.ArgumentParser(description='Deploy FaaSr functions using Terraform')
    parser.add_argument('--workflow-file', required=True,
                      help='Path to the workflow JSON file')
    parser.add_argument('--terraform-dir', default='terraform',
                      help='Path to Terraform directory')
    parser.add_argument('--plan-only', action='store_true',
                      help='Only run terraform plan, do not apply')
    parser.add_argument('--destroy', action='store_true',
                      help='Destroy the infrastructure')
    parser.add_argument('--auto-approve', action='store_true',
                      help='Auto-approve Terraform changes')
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

def detect_git_repository():
    """Auto-detect GitHub repository from git remote"""
    try:
        result = subprocess.run(['git', 'remote', 'get-url', 'origin'], 
                              capture_output=True, text=True, check=True)
        remote_url = result.stdout.strip()
        
        if 'github.com' in remote_url:
            if remote_url.startswith('https://github.com/'):
                repo_path = remote_url.replace('https://github.com/', '').replace('.git', '')
            elif remote_url.startswith('git@github.com:'):
                repo_path = remote_url.replace('git@github.com:', '').replace('.git', '')
            else:
                raise Exception("Unable to parse GitHub repository from remote URL")
            return repo_path
        else:
            raise Exception("Remote URL is not a GitHub repository")
    except Exception as e:
        print(f"Warning: Could not auto-detect repository: {str(e)}")
        return None

def generate_tfvars(workflow_data, workflow_file_path, terraform_dir):
    """Generate terraform.tfvars file from environment and auto-detection"""
    
    # Auto-detect repository
    github_repo = detect_git_repository()
    if not github_repo:
        github_repo = os.getenv('GITHUB_REPOSITORY', 'unknown/unknown')
    
    # Get environment variables
    github_token = os.getenv('PAT')
    aws_lambda_role_arn = os.getenv('AWS_LAMBDA_ROLE_ARN')
    aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    if not github_token:
        print("Warning: PAT environment variable not set - GitHub operations may fail")
    if not aws_lambda_role_arn:
        print("Warning: AWS_LAMBDA_ROLE_ARN environment variable not set - AWS operations may fail")
    
    # Create tfvars content
    tfvars_content = f'''# Auto-generated terraform.tfvars
workflow_file_path = "{os.path.abspath(workflow_file_path)}"
github_repository = "{github_repo}"
aws_region = "{aws_region}"
'''
    
    if github_token:
        tfvars_content += f'github_token = "{github_token}"\n'
    if aws_lambda_role_arn:
        tfvars_content += f'aws_lambda_role_arn = "{aws_lambda_role_arn}"\n'
    
    # Add tags
    json_prefix = Path(workflow_file_path).stem
    tfvars_content += f'''
tags = {{
  Project = "FaaSr"
  Workflow = "{json_prefix}"
  ManagedBy = "Terraform"
}}
'''
    
    # Write to terraform directory
    tfvars_path = Path(terraform_dir) / 'terraform.tfvars'
    with open(tfvars_path, 'w') as f:
        f.write(tfvars_content)
    
    print(f"Generated {tfvars_path}")
    return tfvars_path

def run_terraform_command(command, terraform_dir, auto_approve=False):
    """Run a terraform command in the specified directory"""
    
    # Change to terraform directory
    original_dir = os.getcwd()
    os.chdir(terraform_dir)
    
    try:
        # Prepare the command
        tf_cmd = ['terraform'] + command
        if auto_approve and 'apply' in command:
            tf_cmd.append('-auto-approve')
        
        print(f"Running: {' '.join(tf_cmd)}")
        result = subprocess.run(tf_cmd, check=True)
        return result.returncode == 0
        
    except subprocess.CalledProcessError as e:
        print(f"Terraform command failed with exit code {e.returncode}")
        return False
    except FileNotFoundError:
        print("Error: Terraform not found. Please install Terraform.")
        return False
    finally:
        os.chdir(original_dir)

def handle_openwhisk_deployment(workflow_data):
    """Handle OpenWhisk deployment using the original Python logic"""
    print("OpenWhisk deployment not yet supported in Terraform mode.")
    print("Use the original deploy_functions.py script for OpenWhisk deployment.")
    
    # Check if there are OpenWhisk functions
    ow_functions = []
    for func_name, func_data in workflow_data['FunctionList'].items():
        server_name = func_data['FaaSServer']
        server_config = workflow_data['ComputeServers'][server_name]
        if server_config['FaaSType'].lower() == 'openwhisk':
            ow_functions.append(func_name)
    
    if ow_functions:
        print(f"OpenWhisk functions found: {', '.join(ow_functions)}")
        print("Consider running:")
        print(f"  python scripts/deploy_functions.py --workflow-file {workflow_data.get('_workflow_file', 'your_file.json')}")

def main():
    args = parse_arguments()
    
    # Ensure terraform directory exists
    terraform_dir = Path(args.terraform_dir)
    if not terraform_dir.exists():
        print(f"Error: Terraform directory {terraform_dir} not found")
        print("Please run this script from the project root directory")
        sys.exit(1)
    
    # Read and validate workflow file
    workflow_data = read_workflow_file(args.workflow_file)
    workflow_data['_workflow_file'] = args.workflow_file
    
    # Generate terraform.tfvars
    generate_tfvars(workflow_data, args.workflow_file, terraform_dir)
    
    # Initialize Terraform if needed
    if not (terraform_dir / '.terraform').exists():
        print("Initializing Terraform...")
        if not run_terraform_command(['init'], terraform_dir):
            sys.exit(1)
    
    # Handle different operations
    if args.destroy:
        print("Destroying infrastructure...")
        success = run_terraform_command(['destroy'], terraform_dir, args.auto_approve)
    elif args.plan_only:
        print("Planning changes...")
        success = run_terraform_command(['plan'], terraform_dir)
    else:
        print("Planning changes...")
        if run_terraform_command(['plan'], terraform_dir):
            if args.auto_approve:
                print("Applying changes...")
                success = run_terraform_command(['apply'], terraform_dir, True)
            else:
                response = input("Apply these changes? (yes/no): ")
                if response.lower() in ['yes', 'y']:
                    success = run_terraform_command(['apply'], terraform_dir, False)
                else:
                    print("Deployment cancelled")
                    sys.exit(0)
        else:
            success = False
    
    if success:
        if not args.destroy and not args.plan_only:
            print("\n" + "="*50)
            print("Terraform deployment completed successfully!")
            print("="*50)
            
            # Show outputs
            print("\nDeployment summary:")
            run_terraform_command(['output'], terraform_dir)
            
            # Handle OpenWhisk if needed
            handle_openwhisk_deployment(workflow_data)
        elif args.destroy:
            print("Infrastructure destroyed successfully!")
    else:
        print("Terraform operation failed!")
        sys.exit(1)

if __name__ == '__main__':
    main() 