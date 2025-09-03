#!/usr/bin/env python3

import json
import os
import sys

# Use local scheduler since pip package has syntax errors
# Add the project root to Python path to find local scheduler
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from scheduler.scheduler import Scheduler

# Create a minimal FaaSrPayload class that works with scheduler
class FaaSrPayload:
    def __init__(self, data):
        self.data = data
    
    def __getitem__(self, key):
        return self.data[key]
    
    def __setitem__(self, key, value):
        self.data[key] = value
    
    def __contains__(self, key):
        return key in self.data
    
    def get(self, key, default=None):
        return self.data.get(key, default)



class FaaSrPayloadAdapter:
    """
    Adapter class to bridge invoke_workflow.py data format to scheduler.py expectations
    """
    
    def __init__(self, workflow_data, workflow_file_path=None):
        self.data = workflow_data.copy()
        self.workflow_file_path = workflow_file_path
        
        # Set up required attributes that scheduler expects
        self._setup_credentials()
        self._setup_overwritten_fields()
        self._setup_url()
    
    def _setup_credentials(self):
        """Replace placeholder credentials with actual environment values"""
        credentials = {
            "My_GitHub_Account_TOKEN": os.getenv('GITHUB_TOKEN'),
            "My_Minio_Bucket_ACCESS_KEY": os.getenv('MINIO_ACCESS_KEY'),
            "My_Minio_Bucket_SECRET_KEY": os.getenv('MINIO_SECRET_KEY'),
            "My_OW_Account_API_KEY": os.getenv('OW_API_KEY', ''),
            "My_Lambda_Account_ACCESS_KEY": os.getenv('AWS_ACCESS_KEY_ID', ''),
            "My_Lambda_Account_SECRET_KEY": os.getenv('AWS_SECRET_ACCESS_KEY', ''),
        }
        
        # Replace credentials in ComputeServers
        if 'ComputeServers' in self.data:
            for server_key, server_config in self.data['ComputeServers'].items():
                faas_type = server_config.get('FaaSType', '').lower()
                
                if faas_type in ['lambda', 'aws_lambda', 'aws']:
                    if credentials['My_Lambda_Account_ACCESS_KEY']:
                        server_config['AccessKey'] = credentials['My_Lambda_Account_ACCESS_KEY']
                    if credentials['My_Lambda_Account_SECRET_KEY']:
                        server_config['SecretKey'] = credentials['My_Lambda_Account_SECRET_KEY']
                elif faas_type in ['githubactions', 'github_actions', 'github']:
                    if credentials['My_GitHub_Account_TOKEN']:
                        server_config['Token'] = credentials['My_GitHub_Account_TOKEN']
                elif faas_type in ['openwhisk', 'open_whisk', 'ow']:
                    if credentials['My_OW_Account_API_KEY']:
                        server_config['API.key'] = credentials['My_OW_Account_API_KEY']
        
        # Replace credentials in DataStores
        if 'DataStores' in self.data:
            for store_key, store_config in self.data['DataStores'].items():
                if store_key == 'My_Minio_Bucket':
                    if credentials['My_Minio_Bucket_ACCESS_KEY']:
                        store_config['AccessKey'] = credentials['My_Minio_Bucket_ACCESS_KEY']
                    if credentials['My_Minio_Bucket_SECRET_KEY']:
                        store_config['SecretKey'] = credentials['My_Minio_Bucket_SECRET_KEY']
    
    def _setup_overwritten_fields(self):
        """Setup overwritten fields that scheduler uses for next function triggers"""
        self.overwritten = {
            "FunctionInvoke": self.data.get("FunctionInvoke"),
            "InvocationID": self.data.get("InvocationID", ""),
            "InvocationTimestamp": self.data.get("InvocationTimestamp", ""),
            "WorkflowName": self.data.get("WorkflowName", "default")
        }
        
        # Add other workflow fields
        for key, value in self.data.items():
            if key not in ["ComputeServers", "DataStores", "_workflow_file"]:
                self.overwritten[key] = value
    
    def _setup_url(self):
        """Setup payload URL for GitHub-style workflow passing"""
        if self.workflow_file_path:
            # Extract components for GitHub URL format
            workflow_filename = os.path.basename(self.workflow_file_path)
            
            # Try to build GitHub URL if we have GitHub server config
            github_servers = [
                server for server in self.data.get('ComputeServers', {}).values()
                if server.get('FaaSType', '').lower() in ['githubactions', 'github_actions', 'github']
            ]
            
            if github_servers:
                server = github_servers[0]
                username = server.get('UserName', 'unknown')
                reponame = server.get('ActionRepoName', 'unknown')
                branch = server.get('Branch', 'main')
                self.url = f"{username}/{reponame}/{branch}/{workflow_filename}"
            else:
                self.url = workflow_filename
        else:
            self.url = "unknown"
    
    def __getitem__(self, key):
        """Allow dict-style access to workflow data"""
        return self.data[key]
    
    def __setitem__(self, key, value):
        """Allow dict-style assignment to workflow data"""
        self.data[key] = value
        # Update overwritten fields if it's a workflow field
        if key not in ["ComputeServers", "DataStores", "_workflow_file"]:
            self.overwritten[key] = value
    
    def __contains__(self, key):
        """Allow 'in' operator"""
        return key in self.data
    
    def get(self, key, default=None):
        """Get method like dict"""
        return self.data.get(key, default)
    
    def get_complete_workflow(self):
        """Return complete workflow data (used by OpenWhisk)"""
        return self.data


def migrate_invoke_workflow_to_scheduler(workflow_file_path, function_name=None):
    """
    Migration function that uses scheduler.py exactly as intended
    
    Args:
        workflow_file_path: Path to the workflow JSON file
        function_name: Optional function name to invoke (if None, uses FunctionInvoke from file)
    """
    
    # Read workflow file
    try:
        with open(workflow_file_path, 'r') as f:
            workflow_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Workflow file {workflow_file_path} not found")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in workflow file {workflow_file_path}")
        sys.exit(1)
    
    # Override function name if provided
    if function_name:
        workflow_data['FunctionInvoke'] = function_name
    
    # Get the function to invoke
    function_invoke = workflow_data.get('FunctionInvoke')
    if not function_invoke:
        print("Error: No FunctionInvoke specified in workflow file")
        sys.exit(1)
    
    if function_invoke not in workflow_data['ActionList']:
        print(f"Error: FunctionInvoke '{function_invoke}' not found in ActionList")
        sys.exit(1)
    
    # Apply credential processing using adapter
    adapter = FaaSrPayloadAdapter(workflow_data, workflow_file_path)
    
    # Create FaaSrPayload using the processed data
    faasr_payload = FaaSrPayload(adapter.data)
    
    # Create scheduler instance
    scheduler = Scheduler(faasr_payload)
    
    # Use scheduler.py exactly as intended
    print(f"Triggering function '{function_invoke}' using scheduler...")
    try:
        scheduler.trigger_func(function_invoke)
        print("Function triggered successfully!")
    except Exception as e:
        print(f"Error triggering function: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Migration adapter that uses scheduler.py')
    parser.add_argument('--workflow-file', required=True, help='Path to the workflow JSON file')
    parser.add_argument('--function-name', help='Function name to invoke (optional, uses FunctionInvoke from file if not specified)')
    
    args = parser.parse_args()
    
    migrate_invoke_workflow_to_scheduler(args.workflow_file, args.function_name)