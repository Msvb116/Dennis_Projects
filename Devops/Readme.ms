# Azure DevOps CI/CD Pipelines

This directory contains the pipeline definitions, infrastructure scripts, and configuration templates used to automate the build, testing, and deployment of Dennis Project resources.

## What Belongs in This Folder
* **Pipeline Definitions (`*.yml` / `*.yaml`):** Multi-stage YAML files for Continuous Integration (CI) and Continuous Delivery (CD).
* **Infrastructure as Code (IaC):** Bicep templates, ARM JSON templates, or Terraform configs to provision Azure resources (ADF, Databricks, Storage).
* **Deployment Scripts:** PowerShell (`.ps1`) or Bash (`.sh`) scripts for post-deployment tasks, environment variable swaps, or Databricks workspace CLI deployment.
* **Variable & Environment Files:** Parameter templates (excluding sensitive secrets) for dev, staging, and production target stages.

## Pipeline Workflows
* **CI:** Validates Python scripts, runs unit tests, and checks ADF JSON syntax on pull requests.
* **CD:** Publishes ADF artifacts and synchronizes Databricks code to the target Azure environments upon merging to `main`.
