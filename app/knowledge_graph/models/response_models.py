"""
Response models for the Knowledge Graph API.
These models define the structure of responses from the API endpoints.
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime


class NodeResponse(BaseModel):
    """Response model for a node"""
    id: str
    name: str
    node_type: str
    properties: Dict[str, Any] = Field(default_factory=dict)
    labels: List[str] = Field(default_factory=list)


class RelationshipResponse(BaseModel):
    """Response model for a relationship"""
    id: str
    type: str
    source_id: str
    target_id: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class GraphBuildResponse(BaseModel):
    """Response model for graph build operation"""
    message: str
    nodes_created: int = 0
    relationships_created: int = 0
    execution_time: float = 0.0  # in seconds
    details: Optional[Dict[str, Any]] = None


class NaturalLanguageQueryResponse(BaseModel):
    """Response model for natural language query"""
    message: str
    original_query: str
    interpreted_query: Optional[str] = None
    generated_cypher: Optional[str] = None
    results: List[Dict[str, Any]] = Field(default_factory=list)
    execution_time: float = 0.0  # in seconds
