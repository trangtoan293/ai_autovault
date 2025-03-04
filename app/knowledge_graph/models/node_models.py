"""
Node models for the Knowledge Graph.
These models define the structure of nodes in the Neo4j graph database.
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class NodeType(str, Enum):
    """Enumeration of node types"""
    SOURCE_SYSTEM = "SourceSystem"
    SOURCE_SCHEMA = "SourceSchema"
    TARGET_SCHEMA = "TargetSchema"
    SOURCE_TABLE = "SourceTable"
    TARGET_TABLE = "TargetTable"
    SOURCE_COLUMN = "SourceColumn"
    TARGET_COLUMN = "TargetColumn"
    SCHEMA = "Schema"
    TABLE = "Table"
    COLUMN = "Column"
    DATA_VAULT_COMPONENT = "DataVaultComponent"


class ComponentType(str, Enum):
    """Enumeration of Data Vault component types"""
    HUB = "hub"
    LINK = "link"
    SATELLITE = "satellite"
    LINK_SATELLITE = "link_satellite"


class NodeBase(BaseModel):
    """Base class for all graph nodes"""
    id: Optional[str] = None  # Graph DB ID
    name: str
    node_type: NodeType
    properties: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class SourceSystemNode(NodeBase):
    """Source system node"""
    node_type: NodeType = NodeType.SOURCE_SYSTEM
    description: Optional[str] = None


class SchemaNode(NodeBase):
    """Schema node (legacy - maintained for backward compatibility)"""
    node_type: NodeType = NodeType.SCHEMA
    source_system: str  # Reference to parent source system


class SourceSchemaNode(NodeBase):
    """Source schema node"""
    node_type: NodeType = NodeType.SOURCE_SCHEMA
    source_system: str  # Reference to parent source system


class TargetSchemaNode(NodeBase):
    """Target schema node"""
    node_type: NodeType = NodeType.TARGET_SCHEMA


class SourceTableNode(NodeBase):
    """Source table node"""
    node_type: NodeType = NodeType.SOURCE_TABLE
    schema: str  # Reference to parent schema
    description: Optional[str] = None


class TableNode(NodeBase):
    """Table node (legacy - maintained for backward compatibility)"""
    node_type: NodeType = NodeType.TABLE
    schema: str  # Reference to parent schema
    description: Optional[str] = None


class TargetTableNode(NodeBase):
    """Target table node"""
    node_type: NodeType = NodeType.TARGET_TABLE
    schema: str  # Reference to parent schema
    description: Optional[str] = None
    entity_type: Optional[str] = None  # Type of entity (hub, link, satellite)
    collision_code: Optional[str] = None


class SourceColumnNode(NodeBase):
    """Source column node"""
    node_type: NodeType = NodeType.SOURCE_COLUMN
    table: str  # Reference to parent table
    schema: str  # Reference to parent schema
    data_type: str
    description: Optional[str] = None
    business_definition: Optional[str] = None
    nullable: bool = True
    ordinal_position: Optional[int] = None


class ColumnNode(NodeBase):
    """Column node (legacy - maintained for backward compatibility)"""
    node_type: NodeType = NodeType.COLUMN
    table: str  # Reference to parent table
    schema: str  # Reference to parent schema
    data_type: str
    description: Optional[str] = None
    business_definition: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_table: Optional[str] = None
    foreign_key_column: Optional[str] = None
    nullable: bool = True
    ordinal_position: Optional[int] = None


class TargetColumnNode(NodeBase):
    """Target column node"""
    node_type: NodeType = NodeType.TARGET_COLUMN
    table: str  # Reference to parent table
    schema: str  # Reference to parent schema
    data_type: str
    description: Optional[str] = None
    key_type: Optional[str] = None  # hash_key_hub, biz_key, descriptive, etc.
    nullable: bool = True
    ordinal_position: Optional[int] = None


class TableNode(NodeBase):
    """Table node"""
    node_type: NodeType = NodeType.TABLE
    schema: str  # Reference to parent schema
    description: Optional[str] = None
    
    
class ColumnNode(NodeBase):
    """Column node"""
    node_type: NodeType = NodeType.COLUMN
    table: str  # Reference to parent table
    schema: str  # Reference to parent schema
    data_type: str
    description: Optional[str] = None
    business_definition: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_table: Optional[str] = None
    foreign_key_column: Optional[str] = None
    nullable: bool = True
    ordinal_position: Optional[int] = None


class DataVaultNode(NodeBase):
    """Data Vault component node"""
    node_type: NodeType = NodeType.DATA_VAULT_COMPONENT
    component_type: ComponentType
    description: Optional[str] = None
    source_tables: List[str] = Field(default_factory=list)
    business_keys: List[str] = Field(default_factory=list)
    target_schema: str
