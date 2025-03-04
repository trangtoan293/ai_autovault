"""
API endpoints for the Knowledge Graph module.
These endpoints expose the knowledge graph functionality via REST API.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query, Path, BackgroundTasks, Response, Body
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from app.core.logging import logger
from app.api.dependencies import get_db
from app.core.security import get_current_active_user, User
from app.knowledge_graph.services.graph_builder import GraphBuilder
from app.knowledge_graph.services.llm_service import LLMService
from app.knowledge_graph.services.graph_connector import GraphConnector
from app.knowledge_graph.models.response_models import (
    GraphBuildResponse, NaturalLanguageQueryResponse
)

router = APIRouter()

@router.post("/build-source-metadata", status_code=status.HTTP_201_CREATED, response_model=GraphBuildResponse)
async def build_knowledge_graph(
    source_system: Optional[str] = None,
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user),
    use_enhanced: bool = Query(False, description="Whether to use enhanced node types compatible with build-data-vault")
):
    """
    Build knowledge graph from metadata
    
    Args:
        source_system: Optional name of the source system to filter by
        background_tasks: FastAPI background tasks
        db: Database session
        current_user: Current authenticated user
        use_enhanced: Whether to use enhanced node types compatible with build-data-vault
        
    Returns:
        GraphBuildResponse
    """
    if use_enhanced:
        logger.info(f"User {current_user.username} requested to build knowledge graph using enhanced node types")
        return await build_enhanced_knowledge_graph(source_system, background_tasks, db, current_user)
    
    logger.info(f"User {current_user.username} requested to build knowledge graph")
    
    try:
        builder = GraphBuilder()
        
        # If background tasks are provided, run the build in the background
        if background_tasks:
            logger.info("Running knowledge graph build in background")
            background_tasks.add_task(builder.build_source_metadata_graph, db, source_system)
            return GraphBuildResponse(
                message="Knowledge graph build started in background"
            )
        
        # Otherwise, run synchronously
        result = builder.build_source_metadata_graph(db, source_system)
        
        return GraphBuildResponse(
            message="Knowledge graph built successfully",
            nodes_created=result["summary"]["source_systems_count"] + 
                         result["summary"]["schemas_count"] + 
                         result["summary"]["tables_count"] + 
                         result["summary"]["columns_count"],
            relationships_created=result["summary"]["relationships_count"],
            execution_time=result["summary"]["execution_time"],
            details=result["summary"]
        )
    except Exception as e:
        logger.error(f"Error building knowledge graph: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error building knowledge graph: {str(e)}"
        )


@router.post("/build-source-metadata-enhanced", status_code=status.HTTP_201_CREATED, response_model=GraphBuildResponse)
async def build_enhanced_knowledge_graph(
    source_system: Optional[str] = None,
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Build knowledge graph from metadata using enhanced node types
    that are compatible with build-data-vault
    
    Args:
        source_system: Optional name of the source system to filter by
        background_tasks: FastAPI background tasks
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        GraphBuildResponse
    """
    logger.info(f"User {current_user.username} requested to build enhanced knowledge graph")
    
    try:
        builder = GraphBuilder()
        
        # If background tasks are provided, run the build in the background
        if background_tasks:
            logger.info("Running enhanced knowledge graph build in background")
            background_tasks.add_task(builder.build_source_metadata_graph_enhanced, db, source_system)
            return GraphBuildResponse(
                message="Enhanced knowledge graph build started in background"
            )
        
        # Otherwise, run synchronously
        result = builder.build_source_metadata_graph_enhanced(db, source_system)
        
        # Calculate total nodes created from the appropriate node types
        total_nodes = len(result["summary"]["source_systems"]) + \
                      len(result["summary"]["source_schemas"]) + \
                      len(result["summary"]["source_tables"]) + \
                      len(result["summary"]["source_columns"])
        
        return GraphBuildResponse(
            message="Enhanced knowledge graph built successfully",
            nodes_created=total_nodes,
            relationships_created=result["summary"]["relationships_count"],
            execution_time=result["summary"]["execution_time"],
            details=result["summary"]
        )
    except Exception as e:
        logger.error(f"Error building enhanced knowledge graph: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error building enhanced knowledge graph: {str(e)}"
        )


@router.post("/build-data-vault", status_code=status.HTTP_201_CREATED, response_model=GraphBuildResponse)
async def build_data_vault_graph(
    target_schema: Optional[str] = Query(None, description="Target schema to filter components"),
    target_table: Optional[str] = Query(None, description="Target table to filter components"),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Build Data Vault component graph from data_vault_components based on target_schema or target_table
    
    Args:
        target_schema: Optional target schema to filter components
        target_table: Optional target table to filter components
        background_tasks: FastAPI background tasks
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        GraphBuildResponse
    """
    logger.info(f"User {current_user.username} requested to build Data Vault graph")
    
    if not target_schema and not target_table:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="At least one of target_schema or target_table must be provided"
        )
    
    try:
        builder = GraphBuilder()
        
        # If background tasks are provided, run the build in the background
        if background_tasks:
            logger.info("Running Data Vault graph build in background")
            background_tasks.add_task(
                builder.build_data_vault,
                db,
                target_schema,
                target_table
            )
            return GraphBuildResponse(
                message="Data Vault graph build started in background"
            )
        
        # Otherwise, run synchronously
        result = builder.build_data_vault(db, target_schema, target_table)
        
        return GraphBuildResponse(
            message="Data Vault graph built successfully",
            nodes_created=len(result["details"]["components"]) if "details" in result and "components" in result["details"] else 0,
            relationships_created=len(result["details"]["relationships"]) if "details" in result and "relationships" in result["details"] else 0,
            execution_time=result["summary"]["execution_time"] if "summary" in result and "execution_time" in result["summary"] else 0,
            details=result["summary"] if "summary" in result else {}
        )
    except Exception as e:
        logger.error(f"Error building Data Vault graph: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error building Data Vault graph: {str(e)}"
        )


@router.delete("/clear", status_code=status.HTTP_200_OK)
async def clear_knowledge_graph(
    confirm: bool = Query(False, description="Confirmation flag"),
    current_user: User = Depends(get_current_active_user)
):
    """
    Clear all data from the knowledge graph database.
    Requires admin role and explicit confirmation.
    
    Args:
        confirm: Confirmation flag, must be set to True
        current_user: Current authenticated user
        
    Returns:
        Message confirming deletion
    """
    # Verify user is admin
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only administrators can clear the knowledge graph"
        )
    
    # Verify confirmation flag
    if not confirm:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Confirmation flag must be set to true"
        )
    
    logger.warning(f"User {current_user.username} is clearing the knowledge graph database")
    
    try:
        connector = GraphConnector()
        connector.clear_database()
        connector.close()
        
        return {
            "message": "Knowledge graph database cleared successfully"
        }
    except Exception as e:
        logger.error(f"Error clearing knowledge graph: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error clearing knowledge graph: {str(e)}"
        )


@router.get("/status", status_code=status.HTTP_200_OK)
async def get_knowledge_graph_status(
    current_user: User = Depends(get_current_active_user)
):
    """
    Get status of the knowledge graph database
    
    Args:
        current_user: Current authenticated user
        
    Returns:
        Status information about the knowledge graph
    """
    logger.info(f"User {current_user.username} requested knowledge graph status")
    
    try:
        connector = GraphConnector()
        
        # Get node counts by type
        node_counts_query = """
        MATCH (n)
        WITH labels(n) AS nodeLabels
        UNWIND nodeLabels AS label
        RETURN label, count(*) AS count
        """
        node_counts = connector.execute_cypher(node_counts_query)
        
        # Get relationship counts by type
        rel_counts_query = """
        MATCH ()-[r]->()
        RETURN type(r) AS type, count(*) AS count
        """
        rel_counts = connector.execute_cypher(rel_counts_query)
        
        # Get database size and version info if available
        db_info_query = """
        CALL dbms.components() YIELD name, versions, edition
        RETURN name, versions, edition
        """
        db_info = connector.execute_cypher(db_info_query)
        
        connector.close()
        
        return {
            "message": "Knowledge graph status retrieved successfully",
            "node_counts": node_counts,
            "relationship_counts": rel_counts,
            "database_info": db_info
        }
    except Exception as e:
        logger.error(f"Error getting knowledge graph status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting knowledge graph status: {str(e)}"
        )



@router.post("/query", response_model=NaturalLanguageQueryResponse)
async def natural_language_query(
    query: str,
    current_user: User = Depends(get_current_active_user)
):
    """
    Query the knowledge graph using natural language
    
    Args:
        query: Natural language query
        current_user: Current authenticated user
        
    Returns:
        NaturalLanguageQueryResponse
    """
    logger.info(f"User {current_user.username} submitted natural language query: '{query}'")
    
    try:
        llm_service = LLMService()
        result = await llm_service.natural_language_to_cypher(query)
        
        return NaturalLanguageQueryResponse(
            message="Natural language query executed",
            original_query=query,
            interpreted_query=None,  # Add interpretation if available
            generated_cypher=result["generated_cypher"],
            results=result["results"],
            execution_time=result["execution_time"]
        )
    except Exception as e:
        logger.error(f"Error processing natural language query: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing natural language query: {str(e)}"
        )


@router.post("/build-detailed-data-vault", status_code=status.HTTP_201_CREATED, response_model=GraphBuildResponse)
async def build_detailed_data_vault(
    yaml_content: str = Body(..., description="YAML content for the Data Vault component"),
    source_system: str = Query("Unknown", description="Name of the source system"),
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_active_user)
):
    """
    Build detailed Data Vault graph from YAML content with comprehensive nodes and relationships
    
    Args:
        yaml_content: YAML content describing the Data Vault component
        source_system: Name of the source system
        background_tasks: FastAPI background tasks
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        GraphBuildResponse
    """
    logger.info(f"User {current_user.username} requested to build detailed Data Vault graph")
    
    try:
        builder = GraphBuilder()
        
        # If background tasks are provided, run the build in the background
        if background_tasks:
            logger.info("Running detailed Data Vault graph build in background")
            background_tasks.add_task(
                builder.build_detailed_data_vault,
                db,
                yaml_content,
                source_system
            )
            return GraphBuildResponse(
                message="Detailed Data Vault graph build started in background"
            )
        
        # Otherwise, run synchronously
        result = builder.build_detailed_data_vault(db, yaml_content, source_system)
        
        return GraphBuildResponse(
            message="Detailed Data Vault graph built successfully",
            nodes_created=result["summary"]["nodes_count"],
            relationships_created=result["summary"]["relationships_count"],
            execution_time=result["summary"]["execution_time"],
            details=result["summary"]
        )
    except Exception as e:
        logger.error(f"Error building detailed Data Vault graph: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error building detailed Data Vault graph: {str(e)}"
        )
