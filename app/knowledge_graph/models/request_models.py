"""
Request models for the Knowledge Graph API.
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from enum import Enum


class ComponentType(str, Enum):
    """Enumeration of Data Vault component types"""
    HUB = "HUB"
    LINK = "LINK"
    SATELLITE = "SATELLITE"
    LINK_SATELLITE = "LINK_SATELLITE"
    REFERENCE_TABLE = "REFERENCE_TABLE"


class DataVaultComponentRequest(BaseModel):
    """Request model for a Data Vault component"""
    name: str
    component_type: ComponentType
    description: Optional[str] = None
    source_tables: List[str] = Field(default_factory=list)
    business_keys: Optional[List[str]] = None
    target_schema: Optional[str] = None
    related_hubs: Optional[List[str]] = None  # For links
    hub: Optional[str] = None  # For satellites
    link: Optional[str] = None  # For link satellites
    properties: Dict[str, Any] = Field(default_factory=dict)


class DataVaultBuildRequest(BaseModel):
    """Request model for building Data Vault graph"""
    components: List[DataVaultComponentRequest]
    link_to_source: bool = True


class DataVaultYamlBuildRequest(BaseModel):
    """Request model for building Data Vault graph from yaml content"""
    target_schema: Optional[str] = None
    target_table: Optional[str] = None
