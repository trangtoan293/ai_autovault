"""
Tests for DBT manager functionality
"""
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import os
import tempfile
import asyncio

from app.services.dbt_manager import DBTManagerService
from fastapi import BackgroundTasks


@pytest.fixture
def dbt_manager_service(temp_dir):
    """DBT manager service fixture"""
    service = DBTManagerService()
    # Override dirs for testing
    service.dbt_project_dir = os.path.join(temp_dir, "dbt_project")
    service.dbt_profiles_dir = os.path.join(temp_dir, "dbt_profiles")
    service.templates_dir = os.path.join(os.path.dirname(__file__), "..", "templates", "dbt")
    return service


@pytest.fixture
def background_tasks():
    """Background tasks fixture"""
    return BackgroundTasks()


class TestDBTManager:
    """Test DBT manager functionality"""
    
    @pytest.mark.asyncio
    async def test_init_project(self, dbt_manager_service, background_tasks):
        """Test project initialization"""
        # Initialize project
        result = await dbt_manager_service.init_project(background_tasks)
        
        # Check result
        assert result.status == "success"
        assert result.message == "DBT project initialized successfully"
        
        # Check if directories were created
        assert os.path.exists(dbt_manager_service.dbt_project_dir)
        assert os.path.exists(dbt_manager_service.dbt_profiles_dir)
        
        # Check if files were created
        assert os.path.exists(os.path.join(dbt_manager_service.dbt_project_dir, "dbt_project.yml"))
        assert os.path.exists(os.path.join(dbt_manager_service.dbt_profiles_dir, "profiles.yml"))
        
        # Check if model directories were created
        assert os.path.exists(os.path.join(dbt_manager_service.dbt_project_dir, "models", "hub"))
        assert os.path.exists(os.path.join(dbt_manager_service.dbt_project_dir, "models", "link"))
        assert os.path.exists(os.path.join(dbt_manager_service.dbt_project_dir, "models", "satellite"))
    
    @pytest.mark.asyncio
    async def test_run_models(self, dbt_manager_service, background_tasks):
        """Test running models"""
        # Run models
        result = await dbt_manager_service.run_models(background_tasks)
        
        # Check result
        assert result.status == "running"
        assert isinstance(result.job_id, str)
        assert "dbt run" in result.command
    
    @pytest.mark.asyncio
    async def test_run_models_with_selection(self, dbt_manager_service, background_tasks):
        """Test running models with selection"""
        # Run models
        result = await dbt_manager_service.run_models(background_tasks, models=["hub_test"])
        
        # Check result
        assert result.status == "running"
        assert isinstance(result.job_id, str)
        assert "dbt run" in result.command
        assert "--select" in result.command
    
    @pytest.mark.asyncio
    async def test_test_models(self, dbt_manager_service, background_tasks):
        """Test testing models"""
        # Test models
        result = await dbt_manager_service.test_models(background_tasks)
        
        # Check result
        assert result.status == "running"
        assert isinstance(result.job_id, str)
        assert "dbt test" in result.command
    
    @pytest.mark.asyncio
    async def test_get_job_status(self, dbt_manager_service):
        """Test getting job status"""
        # Add test job
        job_id = "test-job"
        dbt_manager_service.running_jobs[job_id] = {
            "command": "dbt run",
            "status": "completed",
            "output": "Success",
            "error": ""
        }
        
        # Get job status
        result = await dbt_manager_service.get_job_status(job_id)
        
        # Check result
        assert result.job_id == job_id
        assert result.status == "completed"
        assert result.command == "dbt run"
        assert result.results["output"] == "Success"
    
    @pytest.mark.asyncio
    @patch("asyncio.create_subprocess_exec", new_callable=AsyncMock)
    async def test_generate_docs(self, mock_exec, dbt_manager_service):
        """Test generating docs"""
        # Mock subprocess
        mock_process = AsyncMock()
        mock_process.returncode = 0
        mock_process.communicate.return_value = (b"Success", b"")
        mock_exec.return_value = mock_process
        
        # Generate docs
        result = await dbt_manager_service.generate_docs()
        
        # Check result
        assert result["message"] == "DBT documentation generated successfully"
        assert result["docs_url"] == "http://localhost:8080"
    
    @pytest.mark.asyncio
    @patch("app.services.git_manager.GitManagerService.init_repo", new_callable=AsyncMock)
    @patch("app.services.git_manager.GitManagerService.create_branch", new_callable=AsyncMock)
    @patch("app.services.git_manager.GitManagerService.commit_changes", new_callable=AsyncMock)
    async def test_deploy_models(self, mock_commit, mock_create_branch, mock_init, dbt_manager_service, background_tasks):
        """Test deploying models"""
        # Deploy models
        result = await dbt_manager_service.deploy_models(background_tasks)
        
        # Check result
        assert result.status == "success"
        assert "Models committed locally" in result.message
        assert "branch" in result.details
    
    @pytest.mark.asyncio
    async def test_execute_dbt_command(self, dbt_manager_service):
        """Test executing DBT command"""
        # Setup test
        job_id = "test-job"
        cmd = ["echo", "test"]
        dbt_manager_service.running_jobs[job_id] = {
            "command": " ".join(cmd),
            "status": "running",
            "output": "",
            "error": ""
        }
        
        # Execute command
        await dbt_manager_service._execute_dbt_command(job_id, cmd)
        
        # Check result
        assert dbt_manager_service.running_jobs[job_id]["status"] in ["completed", "failed"]
