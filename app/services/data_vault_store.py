"""
Data Vault component storage service
"""
from typing import List, Optional, Dict, Any
from sqlalchemy import create_engine, Column, Integer, String, JSON, Text, DateTime, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
import datetime
import json

from app.core.config import settings
from app.core.logging import logger
from app.models.data_vault import HubComponent, LinkComponent, SatelliteComponent, LinkSatelliteComponent

# Use the same engine as metadata store
from app.services.metadata_store import engine, SessionLocal, Base


# Define SQLAlchemy models
class DataVaultComponentModel(Base):
    """Data Vault component database model"""
    __tablename__ = "data_vault_components"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), index=True)
    component_type = Column(String(50), index=True)  # hub, link, satellite, link_satellite
    description = Column(Text, nullable=True)
    
    # Source information
    source_system = Column(String(255), index=True)  # Source system name
    source_schema = Column(String(255), nullable=True)  # Source schema
    source_table = Column(String(255), index=True)  # Source table name
    source_tables = Column(JSON, nullable=True)  # For backward compatibility
    source_columns = Column(JSON, nullable=True)  # List of source columns used
    
    # Target information
    target_schema = Column(String(255), index=True)  # Data Vault schema name
    target_table = Column(String(255), index=True)  # Generated Data Vault table name
    target_columns = Column(JSON, nullable=True)  # List of target columns
    collision_code = Column(String(50), nullable=True)  # Collision code for target schema
    
    # Common fields
    business_keys = Column(JSON, nullable=True)
    yaml_content = Column(Text, nullable=True)
    
    # Lineage information
    lineage_mapping = Column(JSON, nullable=True)  # Map of source_column -> target_column
    transformation_logic = Column(Text, nullable=True)  # SQL or description of transformation
    
    # Additional fields for Link components
    related_hubs = Column(JSON, nullable=True)
    
    # Additional fields for Satellite components
    hub_name = Column(String(255), nullable=True, index=True)
    
    # Additional fields for LinkSatellite components or Hub references to Link
    link_name = Column(String(255), nullable=True, index=True)
    
    
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


# Create tables if not exist
Base.metadata.create_all(bind=engine)


class DataVaultStoreService:
    """Service for Data Vault component storage operations"""
    
    def _extract_columns_from_yaml(self, yaml_content: str) -> list:
        """
        Extract columns from YAML content if available.
        Returns list of columns or empty list if not found or on error.
        """
        if not yaml_content:
            return []
            
        try:
            import yaml
            yaml_data = yaml.safe_load(yaml_content)
            
            # Check if the expected structure exists
            if not yaml_data or not isinstance(yaml_data, dict):
                return []
                
            # Columns can be at the root level as 'columns' key
            if 'columns' in yaml_data and isinstance(yaml_data['columns'], list):
                return yaml_data['columns']
                
            # Or they could be nested in a target section
            if 'target' in yaml_data and isinstance(yaml_data['target'], dict):
                if 'columns' in yaml_data['target'] and isinstance(yaml_data['target']['columns'], list):
                    return yaml_data['target']['columns']
                    
            # Look for any section that might have columns
            for section_name, section_data in yaml_data.items():
                if isinstance(section_data, dict) and 'columns' in section_data and isinstance(section_data['columns'], list):
                    return section_data['columns']
                    
            # Direct root columns array without a wrapper key
            if all(isinstance(item, dict) for item in yaml_data.values() if isinstance(item, dict)):
                potential_columns = []
                for key, value in yaml_data.items():
                    if isinstance(value, dict) and 'target' in value:
                        potential_columns.append(value)
                if potential_columns:
                    return potential_columns
            
            logger.debug(f"Could not find columns in YAML: {list(yaml_data.keys())}")
            return []
        except Exception as e:
            logger.warning(f"Error extracting columns from YAML: {str(e)}")
            return []
    
    def _extract_source_columns_from_yaml(self, yaml_content: str) -> list:
        """
        Extract source columns from YAML content if available.
        Returns list of deduplicated source column names.
        """
        source_columns = set()  # Use a set to automatically deduplicate
        
        if not yaml_content:
            return []
            
        try:
            import yaml
            yaml_data = yaml.safe_load(yaml_content)
            
            # Extract columns structure first
            columns = []
            
            # Check if the expected structure exists
            if not yaml_data or not isinstance(yaml_data, dict):
                return []
                
            # Columns can be at the root level as 'columns' key
            if 'columns' in yaml_data and isinstance(yaml_data['columns'], list):
                columns = yaml_data['columns']
            # Or they could be nested in a target section
            elif 'target' in yaml_data and isinstance(yaml_data['target'], dict):
                if 'columns' in yaml_data['target'] and isinstance(yaml_data['target']['columns'], list):
                    columns = yaml_data['target']['columns']
            # Look for any section that might have columns
            else:
                for section_name, section_data in yaml_data.items():
                    if isinstance(section_data, dict) and 'columns' in section_data and isinstance(section_data['columns'], list):
                        columns = section_data['columns']
                        break
            
            # Process each column to extract source fields
            for column in columns:
                if not isinstance(column, dict):
                    continue
                    
                if 'source' in column:
                    source = column['source']
                    
                    # Case 1: source is a string
                    if isinstance(source, str):
                        source_columns.add(source.strip())
                    
                    # Case 2: source is a list
                    elif isinstance(source, list):
                        for src in source:
                            if isinstance(src, str):
                                source_columns.add(src.strip())
                    
                    # Case 3: source is a dict with 'name' key
                    elif isinstance(source, dict) and 'name' in source:
                        source_columns.add(source['name'].strip())
            
            # Convert set back to list and sort
            return sorted(list(source_columns))
            
        except Exception as e:
            logger.warning(f"Error extracting source columns from YAML: {str(e)}")
            return []

    def _extract_target_columns_and_business_keys(self, component: Any) -> tuple:
        """
        Extract target_columns and business_keys from component columns structure.
        
        According to the specified logic:
        For Hub:
            - target_columns: all values of 'target' key in 'columns'
            - business_keys: values of 'target' key in 'columns' where key_type is 'biz_key'
        
        For Link, Satellite, Link Satellite:
            - target_columns: all values of 'target' key in 'columns'
            - business_keys: empty list []
            
        Returns:
            tuple: (target_columns, business_keys)
        """
        target_columns = []
        business_keys = []
        component_type = getattr(component, 'component_type', '')
        
        logger.debug(f"Extracting columns for component: {getattr(component, 'name', 'unknown')} of type {component_type}")
        
        # First try to extract from yaml_content if available
        columns = []
        if hasattr(component, 'yaml_content') and component.yaml_content:
            columns = self._extract_columns_from_yaml(component.yaml_content)
            if columns:
                logger.debug(f"Found {len(columns)} columns in yaml_content")
                
        # If no columns found in yaml_content, try the columns attribute        
        if not columns and hasattr(component, 'columns') and isinstance(component.columns, list):
            columns = component.columns
            logger.debug(f"Using {len(columns)} columns from columns attribute")
            
        # Process columns if we found any
        if columns:
            for column in columns:
                if isinstance(column, dict) and 'target' in column:
                    target_name = column['target']
                    target_columns.append(target_name)
                    
                    # For Hub, also check if it's a business key
                    if (component_type == "hub" and 
                        'key_type' in column and column['key_type'] == 'biz_key'):
                        business_keys.append(target_name)
        
        logger.debug(f"Extracted target_columns: {target_columns}")
        
        # Fallback to existing values if columns extraction fails
        if not target_columns:
            existing_targets = getattr(component, 'target_columns', None) or []
            logger.debug(f"Using fallback target_columns: {existing_targets}")
            target_columns = existing_targets
        
        # For non-hub components, ensure business_keys is empty
        if component_type != "hub":
            business_keys = []
            logger.debug("Non-hub component: business_keys set to empty list")
        elif not business_keys:  # For hub, fallback to existing value if extraction fails
            existing_biz_keys = getattr(component, 'business_keys', None) or []
            logger.debug(f"Using fallback business_keys: {existing_biz_keys}")
            business_keys = existing_biz_keys
            
        return target_columns, business_keys
    
    def save_component(self, db: Session, component: Any, source_system: str, source_schema: str=None, 
                     source_table: str=None, target_schema: str=None, collision_code: str=None) -> DataVaultComponentModel:
        """
        Save a Data Vault component to the database
        """
        # Extract target_columns and business_keys from component
        target_columns, business_keys = self._extract_target_columns_and_business_keys(component)
        
        # Extract source columns from YAML content if available
        source_columns = None
        if hasattr(component, 'yaml_content') and component.yaml_content:
            source_columns = self._extract_source_columns_from_yaml(component.yaml_content)
            if source_columns:
                logger.debug(f"Extracted {len(source_columns)} source columns from YAML")
        
        # Create base model with more detailed information
        db_component = DataVaultComponentModel(
            name=component.name,
            component_type=component.component_type,
            description=component.description,
            
            # Source information
            source_system=source_system,
            source_schema=source_schema,
            source_table=source_table,
            source_tables=getattr(component, 'source_tables', None),
            source_columns=source_columns or getattr(component, 'source_columns', None),
            
            # Target information
            target_schema=target_schema,
            target_table=component.name,  # By default, target table name is the component name
            target_columns=target_columns,  # Set from extracted values
            collision_code=collision_code,
            
            # Common fields
            business_keys=business_keys,  # Set from extracted values
            yaml_content=getattr(component, 'yaml_content', None),
            
            # Lineage information
            lineage_mapping=getattr(component, 'lineage_mapping', None),
            transformation_logic=getattr(component, 'transformation_logic', None),
        )
        
        # Add specific fields based on component type
        if component.component_type == "link":
            db_component.related_hubs = getattr(component, 'related_hubs', [])
            
            # Save first to get an ID
            db.add(db_component)
            db.commit()
            db.refresh(db_component)
            
            # Update link references in related hubs
            if getattr(component, 'related_hubs', None):
                for hub_name in component.related_hubs:
                    hub = db.query(DataVaultComponentModel).filter(
                        DataVaultComponentModel.component_type == "hub",
                        DataVaultComponentModel.name == hub_name
                    ).first()
                    
                    if hub and not hub.link_name:  # Only update if link_name is not set
                        hub.link_name = component.name
                        hub.updated_at = datetime.datetime.utcnow()
                        
                db.commit()
                
            # Return the already saved component
            return db_component
        
        elif component.component_type == "satellite":
            # Set hub reference
            db_component.hub_name = getattr(component, 'hub', None)
        
        elif component.component_type == "link_satellite":
            # Set link reference
            db_component.link_name = getattr(component, 'link', None)
            
            
        elif component.component_type == "hub":
            # For hub, target_columns and business_keys are already set by _extract_target_columns_and_business_keys
            pass
        
        # For other component types (not link, which is already saved)
        if component.component_type != "link":
            # Save to database
            db.add(db_component)
            db.commit()
            db.refresh(db_component)
        
        return db_component
    
    def get_components_by_table(self, db: Session, table_name: str) -> List[DataVaultComponentModel]:
        """
        Get all components for a specific table 
        """
        return db.query(DataVaultComponentModel).filter(
            DataVaultComponentModel.source_table == table_name
        ).all()
    
    def get_components_by_source(self, db: Session, source_system: str) -> List[DataVaultComponentModel]:
        """
        Get all components for a specific source system
        """
        return db.query(DataVaultComponentModel).filter(DataVaultComponentModel.source_system == source_system).all()
    
    def get_components_by_source_and_table(self, db: Session, source_system: str, table_name: str) -> List[DataVaultComponentModel]:
        """
        Get all components for a specific source system and table
        """
        return db.query(DataVaultComponentModel).filter(
            DataVaultComponentModel.source_system == source_system,
            DataVaultComponentModel.source_table == table_name
        ).all()
        
    def get_all_components(self, db: Session) -> List[DataVaultComponentModel]:
        """
        Get all Data Vault components across all source systems and tables
        """
        return db.query(DataVaultComponentModel).all()
        
    def get_components_by_type(self, db: Session, component_type: str) -> List[DataVaultComponentModel]:
        """
        Get all components of a specific type (hub, link, satellite, link_satellite)
        """
        # Normalize component type (remove any trailing 's' if present)
        normalized_type = component_type
        if normalized_type.endswith('s') and normalized_type != 'link_satellites':
            normalized_type = normalized_type[:-1]
        if normalized_type == 'link_satellites':
            normalized_type = 'link_satellite'
        
        # Execute query with normalized type
        query = db.query(DataVaultComponentModel).filter(
            DataVaultComponentModel.component_type == normalized_type
        )
        result = query.all()
        
        logger.info(f"Found {len(result)} components of type {normalized_type}")
        return result
        
    def get_components_summary(self, db: Session) -> Dict[str, Any]:
        """
        Get a summary of all Data Vault components grouped by source system and table
        """
        # Get all components
        all_components = self.get_all_components(db)

        # Group by source system and table
        result = {}
        for component in all_components:
            source_key = component.source_system
            table_key = component.source_table
            
            # Initialize source system if not exists
            if source_key not in result:
                result[source_key] = {}
                
            # Initialize table if not exists
            if table_key not in result[source_key]:
                result[source_key][table_key] = {
                    "hubs": [], 
                    "links": [], 
                    "satellites": [], 
                    "link_satellites": []
                }
            
            # Add component to the appropriate list
            if component.component_type == "hub":
                result[source_key][table_key]["hubs"].append({
                    "id": component.id,
                    "name": component.name,
                    "description": component.description,
                    "target_schema": component.target_schema,
                    "target_table": component.target_table
                })
            elif component.component_type == "link":
                result[source_key][table_key]["links"].append({
                    "id": component.id,
                    "name": component.name,
                    "description": component.description,
                    "target_schema": component.target_schema,
                    "target_table": component.target_table
                })
            elif component.component_type == "satellite":
                result[source_key][table_key]["satellites"].append({
                    "id": component.id,
                    "name": component.name,
                    "description": component.description,
                    "target_schema": component.target_schema,
                    "target_table": component.target_table,
                    "hub_name": component.hub_name
                })
            elif component.component_type == "link_satellite":
                result[source_key][table_key]["link_satellites"].append({
                    "id": component.id,
                    "name": component.name,
                    "description": component.description,
                    "target_schema": component.target_schema,
                    "target_table": component.target_table,
                    "link_name": component.link_name
                })

        return result
    
    def get_component_by_name(self, db: Session, name: str) -> Optional[DataVaultComponentModel]:
        """
        Get a component by name
        """
        return db.query(DataVaultComponentModel).filter(DataVaultComponentModel.name == name).first()
    
   
        
    def get_source_to_target_lineage(self, db: Session, source_system: str, source_schema: str=None, 
                                    source_table: str=None) -> List[Dict[str, Any]]:
        """
        Get data lineage from source table to target Data Vault components
        """
        query = db.query(DataVaultComponentModel).filter(
            DataVaultComponentModel.source_system == source_system
        )
        
        if source_schema:
            query = query.filter(DataVaultComponentModel.source_schema == source_schema)
            
        if source_table:
            query = query.filter(DataVaultComponentModel.source_table == source_table)
            
        components = query.all()
        
        lineage_results = []
        for comp in components:
            lineage_info = {
                "source_system": comp.source_system,
                "source_schema": comp.source_schema,
                "source_table": comp.source_table,
                "source_columns": comp.source_columns,
                "target_schema": comp.target_schema,
                "target_table": comp.target_table or comp.name,
                "target_columns": comp.target_columns,
                "component_type": comp.component_type,
                "component_name": comp.name,
                "mapping": comp.lineage_mapping,
                "transformation": comp.transformation_logic,
                "collision_code": comp.collision_code
            }
            lineage_results.append(lineage_info)
        
        return lineage_results

    def get_target_to_source_lineage(self, db: Session, target_schema: str, 
                                    target_table: str) -> Dict[str, Any]:
        """
        Get data lineage from target Data Vault component back to source
        """
        component = db.query(DataVaultComponentModel).filter(
            DataVaultComponentModel.target_schema == target_schema,
            (DataVaultComponentModel.target_table == target_table) | 
            (DataVaultComponentModel.name == target_table)
        ).first()
        
        if not component:
            return None
        
        return {
            "component_type": component.component_type,
            "component_name": component.name,
            "target_schema": component.target_schema,
            "target_table": component.target_table or component.name,
            "target_columns": component.target_columns,
            "source_system": component.source_system,
            "source_schema": component.source_schema,
            "source_table": component.source_table,
            "source_columns": component.source_columns,
            "mapping": component.lineage_mapping,
            "transformation": component.transformation_logic,
            "collision_code": component.collision_code
        }
    
    
 
    
    def convert_to_pydantic(self, db_component: DataVaultComponentModel) -> Any:
        """
        Convert a database model to a Pydantic model
        """
        base_data = {
            "name": db_component.name,
            "component_type": db_component.component_type,
            "description": db_component.description,
            "source_tables": db_component.source_tables,
            "business_keys": db_component.business_keys,
            "yaml_content": db_component.yaml_content,
            "created_at": db_component.created_at,
            "updated_at": db_component.updated_at,
            "source_columns": db_component.source_columns,
            "target_schema": db_component.target_schema,
            "target_table": db_component.target_table,
            "target_columns": db_component.target_columns,
            "collision_code": db_component.collision_code,
            "lineage_mapping": db_component.lineage_mapping,
            "transformation_logic": db_component.transformation_logic
        }
        
        if db_component.component_type == "hub":
            return HubComponent(**base_data)
            
        elif db_component.component_type == "link":
            return LinkComponent(**base_data, related_hubs=db_component.related_hubs)
            
        elif db_component.component_type == "satellite":
            # For satellite components
            return SatelliteComponent(**base_data, hub=db_component.hub_name, 
                                     source_table=db_component.source_table)
            
        elif db_component.component_type == "link_satellite":
            # For link_satellite components
            return LinkSatelliteComponent(**base_data, link=db_component.link_name,
                                         source_table=db_component.source_table)
        
        return base_data
