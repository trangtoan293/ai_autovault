"""
Relationship models for the Knowledge Graph.
These models define the structure of relationships in the Neo4j graph database.
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum


class RelationshipType(str, Enum):
    """Enumeration of relationship types"""
    CONTAINS = "CONTAINS"
    MAPPED_TO = "MAPPED_TO"
    REFERENCES = "REFERENCES"
    SOURCE_OF = "SOURCE_OF"
    DERIVED_FROM = "DERIVED_FROM"
    PART_OF = "PART_OF"
    TRANSFORMS_TO = "TRANSFORMS_TO"


class RelationshipBase(BaseModel):
    """Base class for all graph relationships"""
    id: Optional[str] = None
    relationship_type: RelationshipType
    source_id: str
    target_id: str
    properties: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True


class ContainsRelationship(RelationshipBase):
    """Contains relationship between parent and child entities"""
    relationship_type: RelationshipType = RelationshipType.CONTAINS


class MappedToRelationship(RelationshipBase):
    """Mapped to relationship between source and target columns"""
    relationship_type: RelationshipType = RelationshipType.MAPPED_TO
    transformation: Optional[str] = None  # Transformation logic
    confidence: float = 1.0  # Confidence score for the mapping


class ReferencesRelationship(RelationshipBase):
    """Foreign key relationship between columns"""
    relationship_type: RelationshipType = RelationshipType.REFERENCES


class SourceOfRelationship(RelationshipBase):
    """Source relationship between source column and DV component"""
    relationship_type: RelationshipType = RelationshipType.SOURCE_OF


class DerivedFromRelationship(RelationshipBase):
    """Derived from relationship between columns (for transformations)"""
    relationship_type: RelationshipType = RelationshipType.DERIVED_FROM
    transformation_rule: Optional[str] = None  # SQL or other transformation rule


class PartOfRelationship(RelationshipBase):
    """Part of relationship, e.g., columns part of a business key"""
    relationship_type: RelationshipType = RelationshipType.PART_OF


class TransformsToRelationship(RelationshipBase):
    """Transforms to relationship between source and target entities"""
    relationship_type: RelationshipType = RelationshipType.TRANSFORMS_TO
    transformation_rule: Optional[str] = None  # Transformation logic
    # Metadata được chuyển sang properties để xử lý trong GraphConnector
