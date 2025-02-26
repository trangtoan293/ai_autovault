"""
DBT project management
"""
import os
import uuid
import asyncio
import subprocess
from typing import List, Dict, Any, Optional
from fastapi import BackgroundTasks, HTTPException

from app.core.config import settings
from app.core.logging import logger
from app.utils.template_utils import render_template
from app.models.response import DBTResponse, DBTRunResponse
from app.services.git_manager import GitManagerService


class DBTManagerService:
    """Service for DBT project management"""
    
    def __init__(self):
        self.dbt_project_dir = settings.DBT_PROJECT_DIR
        self.dbt_profiles_dir = settings.DBT_PROFILES_DIR
        self.templates_dir = "../templates/dbt"
        self.git_manager = GitManagerService()
        self.running_jobs = {}
        
        # Create needed directories
        os.makedirs(self.dbt_project_dir, exist_ok=True)
        os.makedirs(self.dbt_profiles_dir, exist_ok=True)
    
    async def clone_repository(self, repo_url: str, target_dir: Optional[str] = None) -> str:
        """
        Clone a repository from GitLab
        """
        if not target_dir:
            target_dir = os.path.join(self.dbt_project_dir, f"repo_{uuid.uuid4().hex[:8]}")
        
        try:
            result = await self.git_manager.clone_repository(repo_url, target_dir)
            return target_dir
        except Exception as e:
            logger.error(f"Error cloning repository: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error cloning repository: {str(e)}")
            
    async def init_project(self, background_tasks: BackgroundTasks, repo_url: Optional[str] = None) -> DBTResponse:
        """
        Initialize a new DBT project, optionally cloning from a repository
        """
        # If repo_url is provided, clone the repository instead of creating a new project
        if repo_url:
            try:
                project_dir = await self.clone_repository(repo_url)
                return DBTResponse(
                    message=f"Repository cloned successfully to {project_dir}",
                    status="success",
                    details={
                        "project_dir": project_dir,
                        "repo_url": repo_url
                    }
                )
            except Exception as e:
                logger.error(f"Error cloning repository: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Error cloning repository: {str(e)}")
        
        # Create directories if they don't exist
        os.makedirs(self.dbt_project_dir, exist_ok=True)
        os.makedirs(self.dbt_profiles_dir, exist_ok=True)
        
        # Generate dbt_project.yml
        project_config = {
            "name": "data_vault_models",
            "version": "1.0.0",
            "config-version": 2,
            "profile": "data_vault",
            "model-paths": ["models"],
            "analysis-paths": ["analyses"],
            "test-paths": ["tests"],
            "seed-paths": ["seeds"],
            "macro-paths": ["macros"],
            "snapshot-paths": ["snapshots"],
            "target-path": "target",
            "clean-targets": ["target", "dbt_packages"],
            "models": {
                "data_vault_models": {
                    "materialized": "table",
                    "hub": {
                        "materialized": "incremental",
                        "tags": ["hub"]
                    },
                    "link": {
                        "materialized": "incremental",
                        "tags": ["link"]
                    },
                    "satellite": {
                        "materialized": "incremental",
                        "tags": ["satellite"]
                    }
                }
            }
        }
        
        # Render project template
        project_yaml = render_template(
            "dbt_project.yml.j2", 
            self.templates_dir, 
            {"project": project_config}
        )
        
        # Write project file
        project_file_path = os.path.join(self.dbt_project_dir, "dbt_project.yml")
        with open(project_file_path, "w") as f:
            f.write(project_yaml)
        
        # Generate profiles.yml
        profiles_config = {
            "data_vault": {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "snowflake",
                        "account": "{{ env_var('DBT_ACCOUNT') }}",
                        "user": "{{ env_var('DBT_USER') }}",
                        "password": "{{ env_var('DBT_PASSWORD') }}",
                        "role": "{{ env_var('DBT_ROLE') }}",
                        "database": "{{ env_var('DBT_DATABASE') }}",
                        "warehouse": "{{ env_var('DBT_WAREHOUSE') }}",
                        "schema": "{{ env_var('DBT_SCHEMA') }}",
                        "threads": 4
                    }
                }
            }
        }
        
        # Render profiles template
        profiles_yaml = render_template(
            "profiles.yml.j2", 
            self.templates_dir, 
            {"profiles": profiles_config}
        )
        
        # Write profiles file
        profiles_file_path = os.path.join(self.dbt_profiles_dir, "profiles.yml")
        with open(profiles_file_path, "w") as f:
            f.write(profiles_yaml)
        
        # Create model directories
        for model_type in ["hub", "link", "satellite"]:
            model_dir = os.path.join(self.dbt_project_dir, "models", model_type)
            os.makedirs(model_dir, exist_ok=True)
        
        return DBTResponse(
            message="DBT project initialized successfully",
            status="success",
            details={
                "project_dir": self.dbt_project_dir,
                "profiles_dir": self.dbt_profiles_dir
            }
        )
    
    async def run_models(
        self, 
        background_tasks: BackgroundTasks, 
        models: Optional[List[str]] = None,
        full_refresh: bool = False
    ) -> DBTRunResponse:
        """
        Run DBT models
        """
        job_id = str(uuid.uuid4())
        
        # Prepare DBT command
        cmd = ["dbt", "run", "--profiles-dir", self.dbt_profiles_dir]
        
        if models:
            models_param = " ".join([f"--select {model}" for model in models])
            cmd.extend(["--select", models_param])
        
        if full_refresh:
            cmd.append("--full-refresh")
        
        # Execute command in background
        background_tasks.add_task(self._execute_dbt_command, job_id, cmd)
        
        # Store job info
        self.running_jobs[job_id] = {
            "command": " ".join(cmd),
            "status": "running",
            "output": "",
            "error": ""
        }
        
        return DBTRunResponse(
            job_id=job_id,
            status="running",
            command=" ".join(cmd),
            results=None
        )
    
    async def test_models(
        self,
        background_tasks: BackgroundTasks,
        models: Optional[List[str]] = None
    ) -> DBTRunResponse:
        """
        Run DBT tests on models
        """
        job_id = str(uuid.uuid4())
        
        # Prepare DBT command
        cmd = ["dbt", "test", "--profiles-dir", self.dbt_profiles_dir]
        
        if models:
            models_param = " ".join([f"--select {model}" for model in models])
            cmd.extend(["--select", models_param])
        
        # Execute command in background
        background_tasks.add_task(self._execute_dbt_command, job_id, cmd)
        
        # Store job info
        self.running_jobs[job_id] = {
            "command": " ".join(cmd),
            "status": "running",
            "output": "",
            "error": ""
        }
        
        return DBTRunResponse(
            job_id=job_id,
            status="running",
            command=" ".join(cmd),
            results=None
        )
    
    async def compile_models(self, background_tasks: BackgroundTasks, models: Optional[List[str]] = None) -> DBTRunResponse:
        """
        Compile DBT models without running them
        """
        job_id = str(uuid.uuid4())
        
        # Prepare DBT command
        cmd = ["dbt", "compile", "--profiles-dir", self.dbt_profiles_dir]
        
        if models:
            models_param = " ".join([f"--select {model}" for model in models])
            cmd.extend(["--select", models_param])
        
        # Execute command in background
        background_tasks.add_task(self._execute_dbt_command, job_id, cmd)
        
        # Store job info
        self.running_jobs[job_id] = {
            "command": " ".join(cmd),
            "status": "running",
            "output": "",
            "error": ""
        }
        
        return DBTRunResponse(
            job_id=job_id,
            status="running",
            command=" ".join(cmd),
            results=None
        )
    
    async def get_job_status(self, job_id: str) -> DBTRunResponse:
        """
        Get the status of a DBT job
        """
        if job_id not in self.running_jobs:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        job = self.running_jobs[job_id]
        
        # Clean up completed jobs older than 1 hour
        self._cleanup_old_jobs()
        
        return DBTRunResponse(
            job_id=job_id,
            status=job["status"],
            command=job["command"],
            results={
                "output": job["output"],
                "error": job["error"]
            }
        )
        
    def _cleanup_old_jobs(self):
        """
        Clean up completed or failed jobs older than 1 hour
        """
        import time
        current_time = time.time()
        jobs_to_remove = []
        
        for job_id, job_info in self.running_jobs.items():
            if job_info.get("status") in ["completed", "failed"] and job_info.get("timestamp", 0) < current_time - 3600:
                jobs_to_remove.append(job_id)
                
        for job_id in jobs_to_remove:
            del self.running_jobs[job_id]
    
    async def generate_docs(self) -> Dict[str, Any]:
        """
        Generate DBT documentation
        """
        cmd = ["dbt", "docs", "generate", "--profiles-dir", self.dbt_profiles_dir]
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.dbt_project_dir
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                logger.error(f"Error generating DBT docs: {stderr.decode()}")
                raise HTTPException(status_code=500, detail=f"Error generating DBT docs: {stderr.decode()}")
            
            # Start docs server
            docs_cmd = ["dbt", "docs", "serve", "--profiles-dir", self.dbt_profiles_dir, "--port", "8080"]
            
            # We start this in a separate process without waiting
            subprocess.Popen(
                docs_cmd,
                cwd=self.dbt_project_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            return {
                "message": "DBT documentation generated successfully",
                "docs_url": "http://localhost:8080"
            }
            
        except Exception as e:
            logger.error(f"Error generating DBT docs: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error generating DBT docs: {str(e)}")
    
    async def copy_yaml_configs(self, yaml_configs: Dict[str, str]) -> Dict[str, str]:
        """
        Copy YAML config files to appropriate locations in the DBT project
        """
        copied_files = {}
        
        for model_name, yaml_content in yaml_configs.items():
            # Determine model type from content (hub, link, satellite)
            model_type = self._determine_model_type(yaml_content)
            model_dir = os.path.join(self.dbt_project_dir, "models", model_type)
            
            # Ensure directory exists
            os.makedirs(model_dir, exist_ok=True)
            
            # Write YAML file
            file_path = os.path.join(model_dir, f"{model_name}.yml")
            with open(file_path, "w") as f:
                f.write(yaml_content)
            
            copied_files[model_name] = file_path
        
        return copied_files
    
    def _determine_model_type(self, yaml_content: str) -> str:
        """
        Determine the model type (hub, link, satellite) from YAML content
        """
        # Simple detection based on YAML content
        if "hub_" in yaml_content.lower() or "h_" in yaml_content.lower():
            return "hub"
        elif "link_" in yaml_content.lower() or "l_" in yaml_content.lower():
            return "link"
        elif "sat_" in yaml_content.lower() or "s_" in yaml_content.lower():
            return "satellite"
        else:
            return "other"  # Default directory
    
    async def deploy_models(self, background_tasks: BackgroundTasks) -> DBTResponse:
        """
        Deploy DBT models to production
        """
        # Create a branch for deployment
        branch_name = f"deploy-{uuid.uuid4().hex[:8]}"
        
        try:
            # Initialize Git repo if not already initialized
            await self.git_manager.init_repo(self.dbt_project_dir)
            
            # Create and checkout new branch
            await self.git_manager.create_branch(branch_name, self.dbt_project_dir)
            
            # Add and commit all changes
            await self.git_manager.commit_changes("Deploy models to production", self.dbt_project_dir)
            
            # Push to remote if configured
            if settings.GIT_REPO_URL:
                await self.git_manager.push_changes(branch_name, self.dbt_project_dir)
                
                # Create pull request if tokens are configured
                pr_details = {}
                if hasattr(settings, 'GITLAB_TOKEN') and settings.GITLAB_TOKEN:
                    try:
                        pr = await self.create_pull_request(
                            branch_name, 
                            "Deploy Data Vault models", 
                            "Automated deployment of Data Vault models generated by the system."
                        )
                        pr_details["pull_request"] = pr
                    except Exception as e:
                        logger.error(f"Error creating pull request: {str(e)}")
                        pr_details["pull_request_error"] = str(e)
                
                return DBTResponse(
                    message="Models deployed successfully",
                    status="success",
                    details={
                        "branch": branch_name,
                        "repo_url": settings.GIT_REPO_URL,
                        **pr_details
                    }
                )
            else:
                return DBTResponse(
                    message="Models committed locally",
                    status="success",
                    details={
                        "branch": branch_name,
                        "note": "Remote repo not configured. Changes committed locally only."
                    }
                )
                
        except Exception as e:
            logger.error(f"Error deploying models: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error deploying models: {str(e)}")
    
    async def create_pull_request(self, branch_name: str, title: str, description: str) -> Dict[str, Any]:
        """
        Create a pull request/merge request in GitLab
        """
        if not hasattr(settings, 'GITLAB_TOKEN') or not settings.GITLAB_TOKEN or not settings.GIT_REPO_URL:
            raise ValueError("GitLab token and repo URL are required for creating pull requests")
        
        try:
            # This would need to use GitLab API via httpx or similar
            import httpx
            
            headers = {"PRIVATE-TOKEN": settings.GITLAB_TOKEN}
            project_id = self._extract_project_id(settings.GIT_REPO_URL)
            
            data = {
                "source_branch": branch_name,
                "target_branch": "main",
                "title": title,
                "description": description
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{settings.GITLAB_API_URL}/projects/{project_id}/merge_requests",
                    headers=headers,
                    json=data
                )
                
                if response.status_code not in (200, 201):
                    logger.error(f"Error creating merge request: {response.text}")
                    raise ValueError(f"Error creating merge request: {response.status_code}")
                
                return response.json()
        except Exception as e:
            logger.error(f"Error creating pull request: {str(e)}")
            raise
            
    def _extract_project_id(self, repo_url: str) -> str:
        """
        Extract project ID from GitLab repo URL
        """
        # Implementation depends on repo URL format
        # Example: 'https://gitlab.com/namespace/project.git'
        import re
        
        # Try to extract project path from URL
        match = re.search(r'gitlab\.com[/:]([^/]+/[^/.]+)', repo_url)
        if match:
            # URL encode the project path
            from urllib.parse import quote
            return quote(match.group(1), safe='')
        
        raise ValueError(f"Could not extract project ID from URL: {repo_url}")
    
    async def _execute_dbt_command(self, job_id: str, cmd: List[str]):
        """
        Execute DBT command and update job status
        """
        try:
            import time
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.dbt_project_dir
            )
            
            stdout, stderr = await process.communicate()
            
            stdout_str = stdout.decode() if stdout else ""
            stderr_str = stderr.decode() if stderr else ""
            
            # Update job status
            if process.returncode == 0:
                self.running_jobs[job_id]["status"] = "completed"
                self.running_jobs[job_id]["output"] = stdout_str
            else:
                self.running_jobs[job_id]["status"] = "failed"
                self.running_jobs[job_id]["error"] = stderr_str
                self.running_jobs[job_id]["output"] = stdout_str
                
            # Add timestamp for cleanup
            self.running_jobs[job_id]["timestamp"] = time.time()
                
        except Exception as e:
            logger.error(f"Error executing DBT command: {str(e)}")
            self.running_jobs[job_id]["status"] = "failed"
            self.running_jobs[job_id]["error"] = str(e)
            
            # Add timestamp for cleanup
            import time
            self.running_jobs[job_id]["timestamp"] = time.time()
