name: ${workflow_name}

on:
  workflow_dispatch:
    inputs:
      PAYLOAD:
        description: 'Payload'
        required: false

jobs:
  run_docker_image:
    runs-on: ubuntu-latest
    container: ${container_image}
    env:
      SECRET_PAYLOAD: $${{ secrets.SECRET_PAYLOAD }}
      GITHUB_PAT: $${{ secrets.PAT }}
      PAYLOAD: $${{ github.event.inputs.PAYLOAD }}
    steps:
    - name: run Rscript
      run: |
        cd /action
        Rscript faasr_${function_name}_invoke_github-actions.R 