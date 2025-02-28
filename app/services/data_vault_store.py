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
    source_tables = Column(JSON, nullable=True)
    business_keys = Column(JSON, nullable=True)
    yaml_content = Column(Text, nullable=True)
    source_system = Column(String(255), index=True)
    table_name = Column(String(255), index=True)
    
    # Additional fields for Link components
    related_hubs = Column(JSON, nullable=True)
    
    # Additional fields for Satellite components
    hub_name = Column(String(255), nullable=True, index=True)
    descriptive_attrs = Column(JSON, nullable=True)
    
    # Additional fields for LinkSatellite components
    link_name = Column(String(255), nullable=True, index=True)
    
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


# Create tables if not exist
Base.metadata.create_all(bind=engine)


class DataVaultStoreService:
    """Service for Data Vault component storage operations"""
    
    def save_component(self, db: Session, component: Any, source_system: str, table_name: str) -> DataVaultComponentModel:
        """
        Save a Data Vault component to the database
        """
        # Create base model
        db_component = DataVaultComponentModel(
            name=component.name,
            component_type=component.component_type,
            description=component.description,
            source_tables=component.source_tables,
            business_keys=component.business_keys,
            yaml_content=component.yaml_content,
            source_system=source_system,
            table_name=table_name
        )
        
        # Add specific fields based on component type
        if component.component_type == "link":
            db_component.related_hubs = component.related_hubs
        
        elif component.component_type == "satellite":
            db_component.hub_name = component.hub
            db_component.descriptive_attrs = component.descriptive_attrs
        
        elif component.component_type == "link_satellite":
            db_component.link_name = component.link
            db_component.descriptive_attrs = component.descriptive_attrs
        
        # Save to database
        db.add(db_component)
        db.commit()
        db.refresh(db_component)
        
        return db_component
    
    def get_components_by_table(self, db: Session, table_name: str) -> List[DataVaultComponentModel]:
        """
        Get all components for a specific table
        """
        return db.query(DataVaultComponentModel).filter(DataVaultComponentModel.table_name == table_name).all()
    
    def get_components_by_source(self, db: Session, source_system: str) -> List[DataVaultComponentModel]:
        """
        Get all components for a specific table
        """
        return db.query(DataVaultComponentModel).filter(DataVaultComponentModel.source_system == source_system).all()
    
    def get_components_by_source_and_table(self, db: Session, source_system: str, table_name: str) -> List[DataVaultComponentModel]:
        """
        Get all components for a specific source system and table
        """
        return db.query(DataVaultComponentModel).filter(
            DataVaultComponentModel.source_system == source_system,
            DataVaultComponentModel.table_name == table_name
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
            table_key = component.table_name
            
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
                    "description": component.description
                })
            elif component.component_type == "link":
                result[source_key][table_key]["links"].append({
                    "id": component.id,
                    "name": component.name,
                    "description": component.description
                })
            elif component.component_type == "satellite":
                result[source_key][table_key]["satellites"].append({
                    "id": component.id,
                    "name": component.name,
                    "description": component.description
                })
            elif component.component_type == "link_satellite":
                result[source_key][table_key]["link_satellites"].append({
                    "id": component.id,
                    "name": component.name,
                    "description": component.description
                })

        return result
    
    def get_component_by_name(self, db: Session, name: str) -> Optional[DataVaultComponentModel]:
        """
        Get a component by name
        """
        return db.query(DataVaultComponentModel).filter(DataVaultComponentModel.name == name).first()
    
    def update_component_yaml(self, db: Session, component_id: int, yaml_content: str) -> Optional[DataVaultComponentModel]:
        """
        Update the YAML content of a component
        """
        db_component = db.query(DataVaultComponentModel).filter(DataVaultComponentModel.id == component_id).first()
        
        if db_component is None:
            return None
            
        db_component.yaml_content = yaml_content
        db_component.updated_at = datetime.datetime.utcnow()
        
        db.commit()
        db.refresh(db_component)
        
        return db_component
    
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
            "updated_at": db_component.updated_at
        }
        
        if db_component.component_type == "hub":
            return HubComponent(**base_data)
            
        elif db_component.component_type == "link":
            return LinkComponent(**base_data, related_hubs=db_component.related_hubs)
            
        elif db_component.component_type == "satellite":
            return SatelliteComponent(**base_data, hub=db_component.hub_name, 
                                     source_table=db_component.table_name,
                                     descriptive_attrs=db_component.descriptive_attrs)
            
        elif db_component.component_type == "link_satellite":
            return LinkSatelliteComponent(**base_data, link=db_component.link_name,
                                         source_table=db_component.table_name,
                                         descriptive_attrs=db_component.descriptive_attrs)
        
        return base_data
