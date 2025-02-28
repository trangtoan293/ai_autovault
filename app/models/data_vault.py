"""
Data Vault component models
"""
from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, Field
from datetime import datetime


class DataVaultComponent(BaseModel):
    """Base class for Data Vault components"""
    name: str = Field(..., description="Name of the component")
    component_type: str = Field(..., description="Type of the component (hub, link, satellite, link_satellite)")
    description: Optional[str] = Field(None, description="Description of the component")
    source_tables: List[str] = Field(default_factory=list, description="Source tables")
    business_keys: List[str] = Field(default_factory=list, description="Business keys")
    yaml_content: Optional[str] = Field(None, description="Generated YAML content")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class HubComponent(DataVaultComponent):
    """Hub component model"""
    component_type: str = "hub"


class LinkComponent(DataVaultComponent):
    """Link component model"""
    component_type: str = "link"
    related_hubs: List[str] = Field(default_factory=list, description="Related hub components")


class SatelliteComponent(DataVaultComponent):
    """Satellite component model"""
    component_type: str = "satellite"
    hub: str = Field(..., description="Related hub component")
    source_table: str = Field(..., description="Source table")
    descriptive_attrs: List[str] = Field(default_factory=list, description="Descriptive attributes")


class LinkSatelliteComponent(DataVaultComponent):
    """Link Satellite component model"""
    component_type: str = "link_satellite"
    link: str = Field(..., description="Related link component")
    source_table: str = Field(..., description="Source table")
    descriptive_attrs: List[str] = Field(default_factory=list, description="Descriptive attributes")


class ManualComponentInput(BaseModel):
    """Manual input for Data Vault components"""
    hubs: Optional[List[HubComponent]] = Field(None, description="Hub components")
    links: Optional[List[LinkComponent]] = Field(None, description="Link components")
    satellites: Optional[List[SatelliteComponent]] = Field(None, description="Satellite components")
    link_satellites: Optional[List[LinkSatelliteComponent]] = Field(None, description="Link Satellite components")
    source_system: str = Field(..., description="Source system")
    table_name: str = Field(..., description="Table name")


class SimpleHub(BaseModel):
    """Simple hub input model that matches LLM output"""
    name: str
    business_keys: List[str]
    source_tables: List[str]
    description: Optional[str] = None


class SimpleLink(BaseModel):
    """Simple link input model that matches LLM output"""
    name: str
    related_hubs: List[str]
    business_keys: List[str]
    source_tables: List[str]
    description: Optional[str] = None


class SimpleSatellite(BaseModel):
    """Simple satellite input model that matches LLM output"""
    name: str
    hub: str
    business_keys: List[str]
    source_table: str
    descriptive_attrs: List[str]


class SimpleLinkSatellite(BaseModel):
    """Simple link satellite input model that matches LLM output"""
    name: str
    link: str
    business_keys: List[str]
    source_table: str
    descriptive_attrs: List[str]


class SimpleManualInput(BaseModel):
    """Simple manual input model that matches LLM output format"""
    hubs: Optional[List[SimpleHub]] = None
    links: Optional[List[SimpleLink]] = None
    satellites: Optional[List[SimpleSatellite]] = None
    link_satellites: Optional[List[SimpleLinkSatellite]] = None
    source_system: Optional[str] = None
    table_name: Optional[str] = None
