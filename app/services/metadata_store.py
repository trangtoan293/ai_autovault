"""
Metadata storage operations
"""
from typing import List, Optional, Dict, Any
from fastapi import UploadFile
from sqlalchemy import create_engine, Column, Integer, String, JSON, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import datetime
from pydantic import TypeAdapter

from app.core.config import settings
from app.models.metadata import MetadataCreate, MetadataResponse, Metadata
from app.services.data_ingestion import DataIngestionService

# Create database engine
engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# Define SQLAlchemy model
class MetadataModel(Base):
    """Metadata database model"""
    __tablename__ = "metadata"
    
    id = Column(Integer, primary_key=True, index=True)
    table_name = Column(String(255), index=True)
    column_name = Column(String(255), index=True)
    data_type = Column(String(100))
    description = Column(Text, nullable=True)
    source_system = Column(String(255), index=True)
    business_definition = Column(Text, nullable=True)
    is_primary_key = Column(Boolean, default=False)
    is_foreign_key = Column(Boolean, default=False)
    foreign_key_table = Column(String(255), nullable=True)
    foreign_key_column = Column(String(255), nullable=True)
    nullable = Column(Boolean, default=True)
    sample_values = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    additional_properties = Column(JSON, nullable=True)


# Create tables
Base.metadata.create_all(bind=engine)


def get_session():
    """Get database session"""
    return SessionLocal()


class MetadataService:
    """Service for metadata storage operations"""
    
    def __init__(self):
        self.data_ingestion_service = DataIngestionService()
    
    async def process_metadata_file(self, file: UploadFile, db: Session) -> MetadataResponse:
        """
        Process metadata file and store in database
        """
        # Process the file
        df = await self.data_ingestion_service.process_file(file)
        
        # Extract metadata
        column_metadata = self.data_ingestion_service.extract_metadata(df)
        
        # Get unique tables from the metadata
        tables = set(meta["table_name"] for meta in column_metadata)
        
        # Create metadata records
        created_records = []
        for column_meta in column_metadata:
            metadata_create = MetadataCreate(
                table_name=column_meta["table_name"],
                column_name=column_meta["column_name"],
                data_type=column_meta["data_type"],
                description=column_meta["description"],
                source_system=column_meta["schema_name"],  # Use schema as source system
                business_definition=column_meta["table_description"],  # Use table description as business definition
                nullable=column_meta["nullable"],
                sample_values=column_meta["sample_values"],
                additional_properties={
                    "table_description": column_meta["table_description"],
                    "schema_name": column_meta["schema_name"],
                    "unique_values": column_meta["unique_values"]
                }
            )
            created = self.create_metadata(db, metadata_create)
            created_records.append(created)
        
        # Convert SQLAlchemy models to Pydantic models
        metadata_adapter = TypeAdapter(List[Metadata])
        pydantic_records = metadata_adapter.validate_python(created_records)
        
        return MetadataResponse(
            message=f"Successfully processed {len(created_records)} columns from {len(tables)} tables in {file.filename}",
            metadata_count=len(created_records),
            table_name=None,  # Multiple tables might be processed
            metadata=pydantic_records
        )
    
    def create_metadata(self, db: Session, metadata: MetadataCreate) -> MetadataModel:
        """
        Create a new metadata entry
        """
        db_metadata = MetadataModel(
            table_name=metadata.table_name,
            column_name=metadata.column_name,
            data_type=metadata.data_type,
            description=metadata.description,
            source_system=metadata.source_system,
            business_definition=metadata.business_definition,
            is_primary_key=metadata.is_primary_key,
            is_foreign_key=metadata.is_foreign_key,
            foreign_key_table=metadata.foreign_key_table,
            foreign_key_column=metadata.foreign_key_column,
            nullable=metadata.nullable,
            sample_values=metadata.sample_values,
            additional_properties=metadata.additional_properties
        )
        db.add(db_metadata)
        db.commit()
        db.refresh(db_metadata)
        return db_metadata
    
    def get_metadata_by_table(self, db: Session, table_name: str) -> List[MetadataModel]:
        """
        Get all metadata entries for a specific table
        """
        return db.query(MetadataModel).filter(MetadataModel.table_name == table_name).all()
    
    def get_metadata_by_source_system(self, db: Session, source_system: str) -> List[MetadataModel]:
        """
        Get all metadata entries for a specific source system
        """
        return db.query(MetadataModel).filter(MetadataModel.source_system == source_system).all()
    
    def get_metadata_by_source_and_table(self, db: Session, source_system: str, table_name: str) -> List[MetadataModel]:
        """
        Get all metadata entries for a specific source system and table
        """
        return db.query(MetadataModel).filter(
            MetadataModel.source_system == source_system,
            MetadataModel.table_name == table_name
        ).all()
    
    def get_all_metadata(
        self, 
        db: Session, 
        skip: int = 0, 
        limit: int = 100,
        source_system: Optional[str] = None
    ) -> List[MetadataModel]:
        """
        Get all metadata entries with optional filtering
        """
        query = db.query(MetadataModel)
        
        if source_system:
            query = query.filter(MetadataModel.source_system == source_system)
            
        return query.offset(skip).limit(limit).all()
    
    def get_metadata(self, db: Session, metadata_id: int) -> Optional[MetadataModel]:
        """
        Get metadata by ID
        """
        return db.query(MetadataModel).filter(MetadataModel.id == metadata_id).first()
    
    def update_metadata(self, db: Session, metadata_id: int, metadata: MetadataCreate) -> Optional[MetadataModel]:
        """
        Update metadata entry
        """
        db_metadata = self.get_metadata(db, metadata_id)
        if db_metadata is None:
            return None
            
        for key, value in metadata.dict().items():
            setattr(db_metadata, key, value)
            
        db_metadata.updated_at = datetime.datetime.utcnow()
        db.commit()
        db.refresh(db_metadata)
        return db_metadata
    
    def delete_metadata(self, db: Session, metadata_id: int) -> Optional[MetadataModel]:
        """
        Delete metadata entry
        """
        db_metadata = self.get_metadata(db, metadata_id)
        if db_metadata is None:
            return None
            
        db.delete(db_metadata)
        db.commit()
        return db_metadata
