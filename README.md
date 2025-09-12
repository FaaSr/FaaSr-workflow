# FaaSr CLI - GitHub Actions Workflows Guide

This repository contains two powerful GitHub Actions workflows for deploying and triggering FaaSr (Function-as-a-Service for R) functions across multiple cloud platforms.

## 🔍 Overview

The FaaSr CLI provides two main workflows:

1. **Register Functions** (`register-workflow.yml`) - Registers your FaaSr functions to supported cloud platforms
2. **Invoke Function** (`invoke-workflow.yml`) - Executes deployed functions with your workflow configuration

### Supported Platforms

- **AWS Lambda** - Serverless compute on Amazon Web Services
- **GitHub Actions** - Native GitHub workflow execution
- **OpenWhisk** - Open-source serverless platform

## 🛠 Prerequisites

Before using these workflows, ensure you have:

1. A fork copy of this repo
2. Appropriate cloud platform accounts and credentials
3. Workflow configuration files (JSON format)
4. Required GitHub repository secrets configured

## ⚙️ Setup

### 1. Repository Secrets

Configure the following secrets in your GitHub repository (`Settings > Secrets and variables > Actions`):

#### Required for all platforms:
- `GITHUB_TOKEN` or `PAT` - GitHub Personal Access Token with workflow permissions
- `MINIO_ACCESS_KEY` - MinIO/S3 access key for data storage
- `MINIO_SECRET_KEY` - MinIO/S3 secret key for data storage

#### AWS Lambda specific:
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `AWS_DEFAULT_REGION` - AWS region (e.g., `us-east-1`)
- `AWS_LAMBDA_ROLE_ARN` - Lambda execution role ARN

#### OpenWhisk specific:
- `OW_API_KEY` - OpenWhisk API key in format `username:password`

### 2. Workflow Configuration Files

Create JSON configuration files in your repository root (examples: `project1.json`). See [Configuration Files](#configuration-files) section for details.

## 🚀 Workflows

### Register Functions Workflow

**File:** `.github/workflows/register-function.yml`

**Purpose:** Registers FaaSr functions to specified cloud platforms based on your workflow configuration.

#### How to Run:

1. Go to your repository's **Actions** tab
2. Select **"Register FaaSr Functions"** workflow
3. Click **"Run workflow"**
4. Enter parameters:
   - **Workflow file name**: Name of your JSON configuration file (default: `project1.json`)
5. Click **"Run workflow"**

#### What it does:

- Reads your workflow configuration file
- Identifies target platforms (Lambda, GitHub Actions, OpenWhisk)
- Installs required dependencies and tools
- Registers workflow to each specified platform:
  - **AWS Lambda**: Creates/updates Lambda functions with container images
  - **GitHub Actions**: Creates workflow files in `.github/workflows/`
  - **OpenWhisk**: Creates/updates actions using the OpenWhisk CLI

#### Example Usage:
```
Workflow file: project1.json
```

### Invoke Function Workflow

**File:** `.github/workflows/invoke-function.yml`

**Purpose:** Executes a registered FaaSr function with your workflow configuration.

#### How to Run:

1. Go to your repository's **Actions** tab
2. Select **"Invoke FaaSr Function"** workflow
3. Click **"Run workflow"**
4. Enter parameters:
   - **Workflow file name**: Name of your JSON configuration file (default: `project1.json`)
   - **Function name**: (Optional) Specific function to trigger
5. Click **"Run workflow"**

#### What it does:

- Reads your workflow configuration file
- Identifies the function to invoke (from `FunctionInvoke` field or parameter)
- Determines the target platform for the function
- Triggers the function execution:
  - **AWS Lambda**: Invokes Lambda function asynchronously
  - **GitHub Actions**: Triggers the deployed workflow via API
  - **OpenWhisk**: Invokes action via REST API

#### Example Usage:
```
Workflow file: payload.json
Function name: (leave empty to use FunctionInvoke from config)
```

## 📄 Configuration Files

Your JSON configuration files should follow [FaaSr Workflow Schema](https://github.com/FaaSr/FaaSr-package/tree/main/schema)

Use these placeholder patterns in your config files:
- `{ServerName}_TOKEN` for GitHub tokens
- `{ServerName}_ACCESS_KEY` and `{ServerName}_SECRET_KEY` for AWS/MinIO
- `{ServerName}_API_KEY` for OpenWhisk

The workflows will automatically replace these with actual values from your repository secrets.

## 🔧 Troubleshooting

### Common Issues:

1. **Missing secrets**: Ensure all required secrets are configured in your repository
2. **Invalid JSON**: Validate your configuration files using a JSON validator
3. **Permission errors**: Check that your tokens/keys have appropriate permissions
4. **Function not found**: Verify function names match between config and registered functions

### Debug Information:

Both workflows provide detailed logging. Check the Actions logs for:
- Configuration file parsing
- Platform-specific registration steps
- Function invocation responses
- Error messages and stack traces

### Getting Help:

1. Check the Actions logs for detailed error messages
2. Verify your configuration file structure
3. Ensure all required secrets are properly set
4. Test with a simple single-function workflow first

---

## 📚 Additional Resources

- [FaaSr Documentation](https://github.com/FaaSr)
- [FaaSr Turorial + JSON Workflow Builder](https://github.com/FaaSr/FaaSr-tutorial)

---

*This guide covers the essential usage of FaaSr Workflow GitHub Actions. For advanced configurations and custom implementations, refer to the source code in the `scripts/` directory.*
