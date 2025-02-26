"""
Metadata data models
"""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime


# Định nghĩa mô hình cho Column (cấp thấp nhất)
class ColumnMetadata(BaseModel):
    """Column level metadata"""
    name: str = Field(..., description="Name of the column")
    data_type: str = Field(..., description="Data type of the column")
    description: Optional[str] = Field(None, description="Description of the column")
    business_definition: Optional[str] = Field(None, description="Business definition of the column")
    is_primary_key: bool = Field(False, description="Whether this column is a primary key")
    is_foreign_key: bool = Field(False, description="Whether this column is a foreign key")
    foreign_key_table: Optional[str] = Field(None, description="Referenced table (if foreign key)")
    foreign_key_column: Optional[str] = Field(None, description="Referenced column (if foreign key)")
    nullable: bool = Field(True, description="Whether this column can contain NULL values")
    sample_values: Optional[List[Any]] = Field(None, description="Sample values from the column")
    ordinal_position: Optional[int] = Field(None, description="Column position in the table")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional column properties")


# Định nghĩa mô hình cho Table (cấp trung gian)
class TableMetadata(BaseModel):
    """Table level metadata"""
    name: str = Field(..., description="Name of the table")
    schema: str = Field(..., description="Schema name containing the table")
    description: Optional[str] = Field(None, description="Description of the table")
    columns: List[ColumnMetadata] = Field(default_factory=list, description="Columns in the table")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional table properties")


# Định nghĩa mô hình cho Source System (cấp cao nhất)
class SourceSystemMetadata(BaseModel):
    """Source system level metadata"""
    name: str = Field(..., description="Name of the source system")
    description: Optional[str] = Field(None, description="Description of the source system")
    tables: List[TableMetadata] = Field(default_factory=list, description="Tables in the source system")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional source system properties")


# Giữ mô hình cũ cho khả năng tương thích ngược
class MetadataBase(BaseModel):
    """Base Metadata model (for backward compatibility)"""
    table_name: str = Field(..., description="Name of the database table")
    column_name: str = Field(..., description="Name of the table column")
    data_type: str = Field(..., description="Data type of the column")
    description: Optional[str] = Field(None, description="Description of the column")
    source_system: str = Field(..., description="Source system of the data")
    business_definition: Optional[str] = Field(None, description="Business definition of the column")
    is_primary_key: bool = Field(False, description="Whether this column is a primary key")
    is_foreign_key: bool = Field(False, description="Whether this column is a foreign key")
    foreign_key_table: Optional[str] = Field(None, description="Referenced table (if foreign key)")
    foreign_key_column: Optional[str] = Field(None, description="Referenced column (if foreign key)")
    nullable: bool = Field(True, description="Whether this column can contain NULL values")
    sample_values: Optional[List[Any]] = Field(None, description="Sample values from the column")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="Additional properties")

    def to_column_metadata(self) -> ColumnMetadata:
        """Convert to new ColumnMetadata model"""
        return ColumnMetadata(
            name=self.column_name,
            data_type=self.data_type,
            description=self.description,
            business_definition=self.business_definition,
            is_primary_key=self.is_primary_key,
            is_foreign_key=self.is_foreign_key,
            foreign_key_table=self.foreign_key_table,
            foreign_key_column=self.foreign_key_column,
            nullable=self.nullable,
            sample_values=self.sample_values,
            additional_properties=self.additional_properties
        )


class MetadataCreate(MetadataBase):
    """Create Metadata model (for backward compatibility)"""
    pass


class MetadataUpdate(BaseModel):
    """Update Metadata model (for backward compatibility)"""
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    data_type: Optional[str] = None
    description: Optional[str] = None
    source_system: Optional[str] = None
    business_definition: Optional[str] = None
    is_primary_key: Optional[bool] = None
    is_foreign_key: Optional[bool] = None
    foreign_key_table: Optional[str] = None
    foreign_key_column: Optional[str] = None
    nullable: Optional[bool] = None
    sample_values: Optional[List[Any]] = None
    additional_properties: Optional[Dict[str, Any]] = None


class Metadata(MetadataBase):
    """Complete Metadata model with database fields (for backward compatibility)"""
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# Define response models for hierarchical metadata
class ColumnMetadataResponse(BaseModel):
    """Response model for column metadata"""
    column: ColumnMetadata
    message: Optional[str] = None


class TableMetadataResponse(BaseModel):
    """Response model for table metadata"""
    table: TableMetadata
    column_count: int
    message: Optional[str] = None


class SourceSystemMetadataResponse(BaseModel):
    """Response model for source system metadata"""
    source_system: SourceSystemMetadata
    table_count: int
    column_count: int
    message: Optional[str] = None


# Complete hierarchical metadata response
class HierarchicalMetadataResponse(BaseModel):
    """Complete hierarchical metadata response"""
    source_systems: List[SourceSystemMetadata]
    source_system_count: int
    table_count: int
    column_count: int
    message: str


# Keep backward compatibility
class MetadataResponse(BaseModel):
    """API response model for metadata operations (legacy)"""
    message: str
    metadata_count: Optional[int] = None
    table_name: Optional[str] = None
    metadata: Optional[List[Metadata]] = None


class MetadataSearchParams(BaseModel):
    """Search parameters for metadata (legacy)"""
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    source_system: Optional[str] = None
    data_type: Optional[str] = None
    is_primary_key: Optional[bool] = None
    is_foreign_key: Optional[bool] = None


# New hierarchical search parameters
class HierarchicalSearchParams(BaseModel):
    """Hierarchical search parameters"""
    source_system_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    data_type: Optional[str] = None
    with_primary_keys_only: Optional[bool] = None
    with_foreign_keys_only: Optional[bool] = None


# Helper function to convert flat metadata to hierarchical
def convert_to_hierarchical(metadata_list: List[Metadata]) -> HierarchicalMetadataResponse:
    """Convert flat metadata list to hierarchical structure"""
    # Create dictionaries to store the hierarchical structure
    source_systems_dict = {}
    
    # Process each metadata item
    for meta in metadata_list:
        source_system_name = meta.source_system
        table_name = meta.table_name
        schema_name = meta.additional_properties.get('schema_name', 'default') if meta.additional_properties else 'default'
        
        # Create source system if it doesn't exist
        if source_system_name not in source_systems_dict:
            source_systems_dict[source_system_name] = SourceSystemMetadata(
                name=source_system_name,
                description=None,
                tables=[],
                additional_properties={}
            )
        
        # Get source system
        source_system = source_systems_dict[source_system_name]
        
        # Find table if it exists
        table = next((t for t in source_system.tables if t.name == table_name and t.schema == schema_name), None)
        
        # Create table if it doesn't exist
        if table is None:
            table_desc = meta.additional_properties.get('table_description', None) if meta.additional_properties else None
            table = TableMetadata(
                name=table_name,
                schema=schema_name,
                description=table_desc,
                columns=[],
                additional_properties={}
            )
            source_system.tables.append(table)
        
        # Create column
        column = meta.to_column_metadata()
        table.columns.append(column)
    
    # Convert dictionaries to lists for response
    source_systems = list(source_systems_dict.values())
    
    # Count totals
    table_count = sum(len(ss.tables) for ss in source_systems)
    column_count = sum(sum(len(table.columns) for table in ss.tables) for ss in source_systems)
    
    # Create response
    return HierarchicalMetadataResponse(
        source_systems=source_systems,
        source_system_count=len(source_systems),
        table_count=table_count,
        column_count=column_count,
        message=f"Retrieved {len(source_systems)} source systems with {table_count} tables and {column_count} columns"
    )
