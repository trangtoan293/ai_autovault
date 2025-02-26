"""
DBT operations endpoints
"""
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query, Body
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.core.security import get_current_active_user, check_admin_permission
from app.models.config import User

from app.api.dependencies import get_db
from app.models.response import DBTResponse, DBTRunResponse
from app.services.dbt_manager import DBTManagerService

router = APIRouter()
dbt_service = DBTManagerService()


class CompileModelRequest(BaseModel):
    """Request model for compiling DBT models"""
    models: Optional[List[str]] = None


class InitProjectRequest(BaseModel):
    """Request model for initializing a DBT project"""
    repo_url: Optional[str] = None


@router.post("/init", response_model=DBTResponse)
async def initialize_dbt_project(
    background_tasks: BackgroundTasks,
    request: InitProjectRequest = Body(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Initialize a new DBT project
    """
    try:
        result = await dbt_service.init_project(background_tasks, request.repo_url)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/run", response_model=DBTRunResponse)
async def run_dbt_models(
    background_tasks: BackgroundTasks,
    models: Optional[List[str]] = Query(None),
    full_refresh: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Run DBT models with optional filtering
    """
    try:
        result = await dbt_service.run_models(background_tasks, models, full_refresh)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/status/{job_id}", response_model=DBTRunResponse)
async def get_dbt_job_status(job_id: str):
    """
    Get the status of a DBT job
    """
    try:
        result = await dbt_service.get_job_status(job_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/compile", response_model=DBTRunResponse)
async def compile_dbt_models(
    background_tasks: BackgroundTasks,
    request: CompileModelRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Compile DBT models without running them
    """
    try:
        result = await dbt_service.compile_models(background_tasks, request.models)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/test", response_model=DBTRunResponse)
async def test_dbt_models(
    background_tasks: BackgroundTasks,
    models: Optional[List[str]] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Run DBT tests on models
    """
    try:
        result = await dbt_service.test_models(background_tasks, models)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/docs", response_model=Dict[str, Any])
async def generate_dbt_docs(
    current_user: User = Depends(get_current_active_user)
):
    """
    Generate DBT documentation
    """
    try:
        result = await dbt_service.generate_docs()
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/deploy", response_model=DBTResponse)
async def deploy_dbt_models(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(check_admin_permission)  # Only admins can deploy
):
    """
    Deploy DBT models to production
    """
    try:
        result = await dbt_service.deploy_models(background_tasks)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
