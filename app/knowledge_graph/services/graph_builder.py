"""
Graph Builder service.
This service builds a knowledge graph from metadata and data vault components.
"""
import time
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session

from app.core.logging import logger
from app.knowledge_graph.services.graph_connector import GraphConnector
from app.knowledge_graph.models.node_models import (
    SourceSystemNode, SchemaNode, TableNode, ColumnNode, DataVaultNode,
    NodeType, ComponentType, SourceSchemaNode, TargetSchemaNode, SourceTableNode,
    TargetTableNode, SourceColumnNode, TargetColumnNode
)
from app.knowledge_graph.models.relationship_models import (
    ContainsRelationship, ReferencesRelationship, SourceOfRelationship,
    MappedToRelationship, RelationshipType, TransformsToRelationship
)
from app.models.metadata import (
    SourceSystemMetadata, TableMetadata, ColumnMetadata, Metadata,
    convert_to_hierarchical
)
from app.models.data_vault import (
    DataVaultComponent, HubComponent, LinkComponent, SatelliteComponent, LinkSatelliteComponent
)
from app.services.metadata_store import MetadataService


class GraphBuilder:
    """Service to build graph knowledge base from metadata"""
    
    def __init__(self):
        """Initialize the graph builder"""
        self.graph = GraphConnector()
        self.metadata_service = MetadataService()
        from app.knowledge_graph.services.node_manager import NodeManagerService
        self.node_manager = NodeManagerService()
    
    def build_source_metadata_graph(
        self, 
        db: Session,
        source_system_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Build source metadata graph from the metadata store with original node types
        (for backward compatibility)
        
        Args:
            db: SQLAlchemy database session
            source_system_name: Optional name of the source system to filter by
            
        Returns:
            Dict containing summary of nodes and relationships created
        """
        start_time = time.time()
        
        # Get metadata from database
        if source_system_name:
            metadata_records = self.metadata_service.get_metadata_by_source_system(db, source_system_name)
        else:
            metadata_records = self.metadata_service.get_all_metadata(db, limit=10000)
        
        # Convert to hierarchical structure
        metadata_list = [Metadata.from_orm(m) for m in metadata_records]
        hierarchical = convert_to_hierarchical(metadata_list)
        
        # Prepare result tracking
        results = {
            "source_systems": [],
            "schemas": [],
            "tables": [],
            "columns": [],
            "relationships": []
        }
        
        logger.info(f"Building graph for {len(hierarchical.source_systems)} source systems with "
                   f"{hierarchical.table_count} tables and {hierarchical.column_count} columns")
        
        # Process each source system
        for source_system in hierarchical.source_systems:
            # Create source system node
            ss_node = SourceSystemNode(
                name=source_system.name,
                description=source_system.description or f"Source system: {source_system.name}"
            )
            ss_id = self.graph.create_node(ss_node)
            results["source_systems"].append({
                "id": ss_id,
                "name": source_system.name
            })
            
            # Process each table
            for table in source_system.tables:
                schema_name = table.schema
                
                # Create schema node
                schema_node = SchemaNode(
                    name=schema_name,
                    source_system=source_system.name
                )
                schema_id = self.graph.create_node(schema_node)
                results["schemas"].append({
                    "id": schema_id,
                    "name": schema_name
                })
                
                # Create relationship source_system -> schema
                contains_rel = ContainsRelationship(
                    source_id=ss_id,
                    target_id=schema_id
                )
                contains_rel_id = self.graph.create_relationship(contains_rel)
                results["relationships"].append({
                    "id": contains_rel_id,
                    "type": RelationshipType.CONTAINS,
                    "source": source_system.name,
                    "target": schema_name
                })
                
                # Create table node
                table_node = TableNode(
                    name=table.name,
                    schema=schema_name,
                    description=table.description
                )
                table_id = self.graph.create_node(table_node)
                results["tables"].append({
                    "id": table_id,
                    "name": table.name
                })
                
                # Create relationship schema -> table
                schema_table_rel = ContainsRelationship(
                    source_id=schema_id,
                    target_id=table_id
                )
                schema_table_rel_id = self.graph.create_relationship(schema_table_rel)
                results["relationships"].append({
                    "id": schema_table_rel_id,
                    "type": RelationshipType.CONTAINS,
                    "source": schema_name,
                    "target": table.name
                })
                
                # Process columns
                for column in table.columns:
                    # Create column node
                    column_node = ColumnNode(
                        name=column.name,
                        table=table.name,
                        schema=schema_name,
                        data_type=column.data_type,
                        description=column.description,
                        business_definition=column.business_definition,
                        nullable=column.nullable,
                        ordinal_position=column.ordinal_position
                    )
                    # Add additional information to properties
                    if column.is_primary_key:
                        column_node.properties["is_primary_key"] = True
                    if column.is_foreign_key:
                        column_node.properties["is_foreign_key"] = True
                        column_node.properties["foreign_key_table"] = column.foreign_key_table
                        column_node.properties["foreign_key_column"] = column.foreign_key_column
                        
                    column_id = self.graph.create_node(column_node)
                    results["columns"].append({
                        "id": column_id,
                        "name": column.name
                    })
                    
                    # Create relationship table -> column
                    table_column_rel = ContainsRelationship(
                        source_id=table_id,
                        target_id=column_id
                    )
                    table_column_rel_id = self.graph.create_relationship(table_column_rel)
                    results["relationships"].append({
                        "id": table_column_rel_id,
                        "type": RelationshipType.CONTAINS,
                        "source": table.name,
                        "target": column.name
                    })
                
                # Create relationships for foreign keys
                for column in table.columns:
                    if column.is_foreign_key and column.foreign_key_table and column.foreign_key_column:
                        # Find the referenced table
                        referenced_table = None
                        for other_table in source_system.tables:
                            if other_table.name == column.foreign_key_table:
                                referenced_table = other_table
                                break
                        
                        if referenced_table:
                            # Find the referenced column
                            referenced_column = None
                            for other_column in referenced_table.columns:
                                if other_column.name == column.foreign_key_column:
                                    referenced_column = other_column
                                    break
                            
                            if referenced_column:
                                # We need to find the node IDs for both columns
                                # This is a simplification - in a real system you would need a more robust way
                                # to find the node IDs for the columns
                                # Get current column node
                                current_column_nodes = self.graph.find_nodes_by_properties(
                                    node_type=NodeType.COLUMN,
                                    properties={
                                        "name": column.name,
                                        "table": table.name,
                                        "schema": schema_name
                                    }
                                )
                                
                                if current_column_nodes:
                                    current_column_id = current_column_nodes[0]["id"]
                                    
                                    # Get referenced column node
                                    ref_column_nodes = self.graph.find_nodes_by_properties(
                                        node_type=NodeType.COLUMN,
                                        properties={
                                            "name": referenced_column.name,
                                            "table": referenced_table.name,
                                            "schema": schema_name
                                        }
                                    )
                                    
                                    if ref_column_nodes:
                                        ref_column_id = ref_column_nodes[0]["id"]
                                        
                                        # Create REFERENCES relationship
                                        ref_rel = ReferencesRelationship(
                                            source_id=current_column_id,
                                            target_id=ref_column_id
                                        )
                                        ref_rel_id = self.graph.create_relationship(ref_rel)
                                        results["relationships"].append({
                                            "id": ref_rel_id,
                                            "type": RelationshipType.REFERENCES,
                                            "source": f"{table.name}.{column.name}",
                                            "target": f"{referenced_table.name}.{referenced_column.name}"
                                        })
        
        execution_time = time.time() - start_time
        logger.info(f"Graph build completed in {execution_time:.2f} seconds")
        
        # Summarize results
        summary = {
            "source_systems_count": len(results["source_systems"]),
            "schemas_count": len(results["schemas"]),
            "tables_count": len(results["tables"]),
            "columns_count": len(results["columns"]),
            "relationships_count": len(results["relationships"]),
            "execution_time": execution_time,
            "source_systems": [system["name"] for system in results["source_systems"]],
            "schemas": [schema["name"] for schema in results["schemas"]],
            "tables": [table["name"] for table in results["tables"]]
        }
        
        return {
            "summary": summary,
            "details": results
        }
    
    def build_source_metadata_graph_enhanced(
        self, 
        db: Session,
        source_system_name: Optional[str] = None,
        node_cache: Dict = None
    ) -> Dict[str, Any]:
        """
        Build source metadata graph from the metadata store with enhanced node types
        compatible with build-data-vault results
        
        Args:
            db: SQLAlchemy database session
            source_system_name: Optional name of the source system to filter by
            node_cache: Optional cache of existing nodes to avoid duplicates
            
        Returns:
            Dict containing summary of nodes and relationships created
        """
        start_time = time.time()
        
        # Initialize node cache if not provided
        if node_cache is None:
            node_cache = {
                "source_systems": {},  # {name: id}
                "source_schemas": {},  # {name: id}
                "source_tables": {},   # {schema.name: id}
                "source_columns": {},  # {table.schema.name: id}
            }
        
        # Get metadata from database
        if source_system_name:
            metadata_records = self.metadata_service.get_metadata_by_source_system(db, source_system_name)
        else:
            metadata_records = self.metadata_service.get_all_metadata(db, limit=10000)
        
        # Convert to hierarchical structure
        metadata_list = [Metadata.from_orm(m) for m in metadata_records]
        hierarchical = convert_to_hierarchical(metadata_list)
        
        # Prepare result tracking
        results = {
            "nodes": [],
            "relationships": []
        }
        
        logger.info(f"Building enhanced graph for {len(hierarchical.source_systems)} source systems with "
                   f"{hierarchical.table_count} tables and {hierarchical.column_count} columns")
        
        # Process each source system
        for source_system in hierarchical.source_systems:
            # Create or reuse source system node using NodeManagerService
            ss_id = self.node_manager.get_or_create_source_system_node(
                name=source_system.name,
                description=source_system.description or f"Source system: {source_system.name}"
            )
            node_cache["source_systems"][source_system.name] = ss_id
            
            # Add to results if not already there
            if not any(node["id"] == ss_id for node in results["nodes"]):
                results["nodes"].append({
                    "id": ss_id,
                    "name": source_system.name,
                    "type": NodeType.SOURCE_SYSTEM
                })
            
            # Process each table
            for table in source_system.tables:
                schema_name = table.schema
                
                # Create or reuse schema node using NodeManagerService
                schema_id = self.node_manager.get_or_create_source_schema_node(
                    name=schema_name,
                    source_system=source_system.name
                )
                node_cache["source_schemas"][schema_name] = schema_id
                
                # Add to results if not already there
                if not any(node["id"] == schema_id for node in results["nodes"]):
                    results["nodes"].append({
                        "id": schema_id,
                        "name": schema_name,
                        "type": NodeType.SOURCE_SCHEMA
                    })
                    
                    # Create relationship source_system -> schema
                    contains_rel = ContainsRelationship(
                        source_id=ss_id,
                        target_id=schema_id
                    )
                    # Add relationship_type to properties for clarity
                    contains_rel.properties["relationship_type"] = "CONTAINS"
                    contains_rel_id = self.graph.create_relationship(contains_rel)
                    results["relationships"].append({
                        "id": contains_rel_id,
                        "type": RelationshipType.CONTAINS,
                        "source": source_system.name,
                        "target": schema_name
                    })
                
                # Create or reuse table node using NodeManagerService
                table_id = self.node_manager.get_or_create_source_table_node(
                    name=table.name,
                    schema=schema_name,
                    description=table.description
                )
                table_key = f"{schema_name}.{table.name}"
                node_cache["source_tables"][table_key] = table_id
                
                # Add to results if not already there
                if not any(node["id"] == table_id for node in results["nodes"]):
                    results["nodes"].append({
                        "id": table_id,
                        "name": table.name,
                        "type": NodeType.SOURCE_TABLE
                    })
                    
                    # Create relationship schema -> table
                    schema_table_rel = ContainsRelationship(
                        source_id=schema_id,
                        target_id=table_id
                    )
                    # Add relationship_type to properties for clarity
                    schema_table_rel.properties["relationship_type"] = "CONTAINS"
                    schema_table_rel_id = self.graph.create_relationship(schema_table_rel)
                    results["relationships"].append({
                        "id": schema_table_rel_id,
                        "type": RelationshipType.CONTAINS,
                        "source": schema_name,
                        "target": table.name
                    })
                
                # Process columns
                for column in table.columns:
                    # Create or reuse column node using NodeManagerService
                    # Create additional properties dict
                    column_props = {
                        "data_type": column.data_type,
                        "description": column.description,
                        "business_definition": column.business_definition,
                        "nullable": column.nullable,
                        "ordinal_position": column.ordinal_position
                    }
                    
                    # Add primary and foreign key information
                    if column.is_primary_key:
                        column_props["is_primary_key"] = True
                    if column.is_foreign_key:
                        column_props["is_foreign_key"] = True
                        column_props["foreign_key_table"] = column.foreign_key_table
                        column_props["foreign_key_column"] = column.foreign_key_column
                    
                    column_id = self.node_manager.get_or_create_source_column_node(
                        name=column.name,
                        table=table.name,
                        schema=schema_name,
                        **column_props
                    )
                    
                    column_key = f"{schema_name}.{table.name}.{column.name}"
                    node_cache["source_columns"][column_key] = column_id
                    
                    # Add to results if not already there
                    if not any(node["id"] == column_id for node in results["nodes"]):
                        results["nodes"].append({
                            "id": column_id,
                            "name": column.name,
                            "type": NodeType.SOURCE_COLUMN
                        })
                        
                        # Create relationship table -> column
                        table_column_rel = ContainsRelationship(
                            source_id=table_id,
                            target_id=column_id
                        )
                        # Add relationship_type to properties for clarity
                        table_column_rel.properties["relationship_type"] = "CONTAINS"
                        table_column_rel_id = self.graph.create_relationship(table_column_rel)
                        results["relationships"].append({
                            "id": table_column_rel_id,
                            "type": RelationshipType.CONTAINS,
                            "source": table.name,
                            "target": column.name
                        })
                
                # Create relationships for foreign keys
                for column in table.columns:
                    if column.is_foreign_key and column.foreign_key_table and column.foreign_key_column:
                        # Get current column ID
                        current_column_key = f"{schema_name}.{table.name}.{column.name}"
                        current_column_id = node_cache["source_columns"][current_column_key]
                        
                        # Find the referenced table
                        referenced_table = None
                        for other_table in source_system.tables:
                            if other_table.name == column.foreign_key_table:
                                referenced_table = other_table
                                break
                        
                        if referenced_table:
                            # Find the referenced column
                            referenced_column = None
                            for other_column in referenced_table.columns:
                                if other_column.name == column.foreign_key_column:
                                    referenced_column = other_column
                                    break
                            
                            if referenced_column:
                                # Get referenced column ID
                                referenced_column_key = f"{schema_name}.{referenced_table.name}.{referenced_column.name}"
                                if referenced_column_key in node_cache["source_columns"]:
                                    ref_column_id = node_cache["source_columns"][referenced_column_key]
                                    
                                    # Create REFERENCES relationship
                                    ref_rel = ReferencesRelationship(
                                        source_id=current_column_id,
                                        target_id=ref_column_id
                                    )
                                    # Add relationship_type to properties for clarity
                                    ref_rel.properties["relationship_type"] = "REFERENCES"
                                    ref_rel_id = self.graph.create_relationship(ref_rel)
                                    results["relationships"].append({
                                        "id": ref_rel_id,
                                        "type": RelationshipType.REFERENCES,
                                        "source": f"{table.name}.{column.name}",
                                        "target": f"{referenced_table.name}.{referenced_column.name}"
                                    })
        
        execution_time = time.time() - start_time
        logger.info(f"Enhanced graph build completed in {execution_time:.2f} seconds")
        
        # Summarize results
        summary = {
            "nodes_count": len(results["nodes"]),
            "relationships_count": len(results["relationships"]),
            "execution_time": execution_time,
            "source_systems": [node["name"] for node in results["nodes"] if node["type"] == NodeType.SOURCE_SYSTEM],
            "source_schemas": [node["name"] for node in results["nodes"] if node["type"] == NodeType.SOURCE_SCHEMA],
            "source_tables": [node["name"] for node in results["nodes"] if node["type"] == NodeType.SOURCE_TABLE],
            "source_columns": [node["name"] for node in results["nodes"] if node["type"] == NodeType.SOURCE_COLUMN]
        }
        
        return {
            "summary": summary,
            "details": results,
            "node_cache": node_cache
        }
    
    def build_data_vault_graph(
        self, 
        db: Session,
        components: List[DataVaultComponent],
        link_to_source: bool = True
    ) -> Dict[str, Any]:
        """
        Build Data Vault component graph and link to source metadata
        
        Args:
            db: SQLAlchemy database session
            components: List of Data Vault components
            link_to_source: Whether to link components to source metadata
            
        Returns:
            Dict containing summary of nodes and relationships created
        """
        start_time = time.time()
        
        # Prepare result tracking
        results = {
            "components": [],
            "relationships": []
        }
        
        # Track component ids by name
        component_ids = {}
        
        # Process each component
        for component in components:
            # Create component node
            component_node = DataVaultNode(
                name=component.name,
                component_type=component.component_type,
                description=component.description,
                source_tables=component.source_tables,
                business_keys=component.business_keys,
                target_schema=component.target_schema
            )
            component_id = self.graph.create_node(component_node)
            component_ids[component.name] = component_id
            results["components"].append({
                "id": component_id, 
                "name": component.name,
                "type": component.component_type
            })
        
        # Create relationships between components
        for component in components:
            component_id = component_ids.get(component.name)
            if not component_id:
                continue
                
            # For links, create relationships to hubs
            if isinstance(component, LinkComponent) and hasattr(component, "related_hubs"):
                for hub_name in component.related_hubs:
                    hub_id = component_ids.get(hub_name)
                    if hub_id:
                        # Create PART_OF relationship from Link to Hub
                        rel = ContainsRelationship(
                            source_id=hub_id,
                            target_id=component_id
                        )
                        rel_id = self.graph.create_relationship(rel)
                        results["relationships"].append({
                            "id": rel_id,
                            "type": RelationshipType.CONTAINS,
                            "source": hub_name,
                            "target": component.name
                        })
            
            # For satellites, create relationships to hubs
            if isinstance(component, SatelliteComponent) and hasattr(component, "hub"):
                hub_id = component_ids.get(component.hub)
                if hub_id:
                    # Create PART_OF relationship from Satellite to Hub
                    rel = ContainsRelationship(
                        source_id=hub_id,
                        target_id=component_id
                    )
                    rel_id = self.graph.create_relationship(rel)
                    results["relationships"].append({
                        "id": rel_id,
                        "type": RelationshipType.CONTAINS,
                        "source": component.hub,
                        "target": component.name
                    })
                    
            # For link satellites, create relationships to links
            if isinstance(component, LinkSatelliteComponent) and hasattr(component, "link"):
                link_id = component_ids.get(component.link)
                if link_id:
                    # Create PART_OF relationship from LinkSatellite to Link
                    rel = ContainsRelationship(
                        source_id=link_id,
                        target_id=component_id
                    )
                    rel_id = self.graph.create_relationship(rel)
                    results["relationships"].append({
                        "id": rel_id,
                        "type": RelationshipType.CONTAINS,
                        "source": component.link,
                        "target": component.name
                    })
        
        # Link to source metadata if requested
        if link_to_source:
            self._link_components_to_source(db, components, component_ids, results)
        
        execution_time = time.time() - start_time
        logger.info(f"Data Vault graph build completed in {execution_time:.2f} seconds")
        
        # Summarize results
        summary = {
            "components_count": len(results["components"]),
            "relationships_count": len(results["relationships"]),
            "execution_time": execution_time
        }
        
        return {
            "summary": summary,
            "details": results
        }
    
    def _link_components_to_source(
        self, 
        db: Session,
        components: List[DataVaultComponent],
        component_ids: Dict[str, str],
        results: Dict[str, List[Dict[str, Any]]]
    ) -> None:
        """
        Link Data Vault components to source metadata
        
        Args:
            db: SQLAlchemy database session
            components: List of Data Vault components
            component_ids: Dict mapping component names to node IDs
            results: Dict tracking results to update
        """
        logger.info("Linking Data Vault components to source metadata")
        
        # Process each component
        for component in components:
            component_id = component_ids.get(component.name)
            if not component_id:
                continue
            
            # Get source tables for this component
            source_tables = component.source_tables
            
            # For each source table, find corresponding columns
            for source_table in source_tables:
                # Find source table node
                source_nodes = self.graph.find_nodes_by_properties(
                    node_type=NodeType.TABLE,
                    properties={"name": source_table}
                )
                
                if not source_nodes:
                    logger.warning(f"Source table {source_table} not found in graph")
                    continue
                
                source_table_id = source_nodes[0]["id"]
                
                # Create SOURCE_OF relationship
                rel = SourceOfRelationship(
                    source_id=source_table_id,
                    target_id=component_id
                )
                rel_id = self.graph.create_relationship(rel)
                results["relationships"].append({
                    "id": rel_id,
                    "type": RelationshipType.SOURCE_OF,
                    "source": source_table,
                    "target": component.name
                })
                
                # For business keys, find corresponding columns and link them
                if hasattr(component, "business_keys") and component.business_keys:
                    for bkey in component.business_keys:
                        # Find corresponding column nodes
                        # Note: This is a simplification - in a real system you might need
                        # more logic to match business keys to specific columns
                        column_nodes = self.graph.execute_cypher(
                            """
                            MATCH (t:Table {name: $table_name})-[:CONTAINS]->(c:Column)
                            WHERE c.name = $column_name
                            RETURN id(c) as id
                            """,
                            params={"table_name": source_table, "column_name": bkey}
                        )
                        
                        for column_node in column_nodes:
                            column_id = column_node["id"]
                            
                            # Create SOURCE_OF relationship from column to component
                            rel = SourceOfRelationship(
                                source_id=column_id,
                                target_id=component_id
                            )
                            rel_id = self.graph.create_relationship(rel)
                            results["relationships"].append({
                                "id": rel_id,
                                "type": RelationshipType.SOURCE_OF,
                                "source": f"{source_table}.{bkey}",
                                "target": component.name
                            })
    
    def build_data_vault(self, db: Session, target_schema: Optional[str] = None, target_table: Optional[str] = None) -> Dict[str, Any]:
        """
        Build Data Vault components based on target_schema or target_table using yaml_content from data_vault_components table
        
        Args:
            db: SQLAlchemy database session
            target_schema: Optional target schema to filter components
            target_table: Optional target table to filter components
            
        Returns:
            Dict containing summary of components built
        """
        from app.services.data_vault_store import DataVaultStoreService, DataVaultComponentModel
        import yaml
        
        start_time = time.time()
        logger.info(f"Building Data Vault components for schema={target_schema}, table={target_table}")
        
        # Get Data Vault store service
        dv_store = DataVaultStoreService()
        
        # Query data_vault_components based on filters
        query = db.query(DataVaultComponentModel)
        
        if target_schema:
            query = query.filter(DataVaultComponentModel.target_schema == target_schema)
            
        if target_table:
            query = query.filter(
                (DataVaultComponentModel.target_table == target_table) | 
                (DataVaultComponentModel.name == target_table)
            )
            
        # Get components from database
        db_components = query.all()
        
        if not db_components:
            logger.warning(f"No Data Vault components found for schema={target_schema}, table={target_table}")
            return {
                "summary": {
                    "components_count": 0,
                    "relationships_count": 0,
                    "execution_time": time.time() - start_time
                },
                "details": {
                    "components": [],
                    "relationships": []
                }
            }
        
        logger.info(f"Found {len(db_components)} Data Vault components")
        
        # Initialize node cache to avoid duplicates
        node_cache = {
            "source_schemas": {},  # {name: id}
            "target_schemas": {},  # {name: id}
            "source_tables": {},   # {schema.name: id}
            "target_tables": {},   # {schema.name: id}
            "source_columns": {},  # {table.schema.name: id}
            "target_columns": {}   # {table.schema.name: id}
        }
        
        # For each component, use build_detailed_data_vault with its yaml_content
        all_results = {
            "summary": {
                "components_count": 0,
                "relationships_count": 0,
                "execution_time": 0,
                "processed_components": []
            },
            "details": {
                "nodes": [],
                "relationships": []
            }
        }
        
        for db_component in db_components:
            try:
                # Only process components with yaml_content
                if not db_component.yaml_content:
                    logger.warning(f"Component {db_component.name} has no yaml_content, skipping")
                    continue
                
                # Use build_detailed_data_vault for each component
                logger.info(f"Building detailed graph for component {db_component.name}")
                source_system = db_component.source_system or "Unknown"
                
                # Gửi node_cache để tái sử dụng các node đã tồn tại
                result = self.build_detailed_data_vault_with_cache(
                    db, 
                    db_component.yaml_content, 
                    source_system, 
                    node_cache
                )
                
                # Merge results
                all_results["summary"]["components_count"] += 1
                all_results["summary"]["relationships_count"] += result["summary"]["relationships_count"]
                all_results["summary"]["processed_components"].append({
                    "name": db_component.name,
                    "type": db_component.component_type,
                    "target_schema": db_component.target_schema,
                    "target_table": db_component.target_table or db_component.name
                })
                
                # Chỉ thêm node mới (chưa tồn tại trong all_results["details"]["nodes"])
                for node in result["details"]["nodes"]:
                    # Kiểm tra xem node đã tồn tại trong kết quả chưa
                    exists = False
                    for existing_node in all_results["details"]["nodes"]:
                        if existing_node["id"] == node["id"]:
                            exists = True
                            break
                    
                    if not exists:
                        all_results["details"]["nodes"].append(node)
                
                # Thêm tất cả relationships
                for rel in result["details"]["relationships"]:
                    all_results["details"]["relationships"].append(rel)
                
            except Exception as e:
                logger.error(f"Error processing component {db_component.name}: {str(e)}")
                continue
        
        execution_time = time.time() - start_time
        all_results["summary"]["execution_time"] = execution_time
        logger.info(f"Completed building Data Vault graph in {execution_time:.2f} seconds")
        
        return all_results
        
    def build_detailed_data_vault_with_cache(self, db: Session, yaml_content: str, source_system_name: str = "Unknown", node_cache: Dict = None) -> Dict[str, Any]:
        """
        Build detailed Data Vault graph from YAML content with comprehensive nodes and relationships, using node cache to avoid duplicates
        
        Args:
            db: SQLAlchemy database session
            yaml_content: YAML content describing the Data Vault component
            source_system_name: Name of the source system
            node_cache: Cache of existing nodes to avoid duplicates
            
        Returns:
            Dict containing summary of nodes and relationships created
        """
        import yaml
        
        # Initialize node cache if not provided
        if node_cache is None:
            node_cache = {
                "source_schemas": {},  # {name: id}
                "target_schemas": {},  # {name: id}
                "source_tables": {},   # {schema.name: id}
                "target_tables": {},   # {schema.name: id}
                "source_columns": {},  # {table.schema.name: id}
                "target_columns": {}   # {table.schema.name: id}
            }
        
        start_time = time.time()
        logger.info(f"Building detailed Data Vault graph from YAML content using cache")
        
        # Parse YAML content
        try:
            dv_config = yaml.safe_load(yaml_content)
            if not dv_config or not isinstance(dv_config, dict):
                raise ValueError("Invalid YAML content structure")
        except Exception as e:
            logger.error(f"Error parsing YAML content: {str(e)}")
            raise ValueError(f"Failed to parse YAML content: {str(e)}")
            
        # Extract basic information
        source_schema = dv_config.get('source_schema')
        source_table = dv_config.get('source_table')
        target_schema = dv_config.get('target_schema')
        target_table = dv_config.get('target_table')
        entity_type = dv_config.get('target_entity_type', 'hub')  # Default to hub
        collision_code = dv_config.get('collision_code')
        description = dv_config.get('description', '')
        
        if not all([source_schema, source_table, target_schema, target_table]):
            raise ValueError("Missing required fields in YAML content")
            
        # Prepare result tracking
        results = {
            "nodes": [],
            "relationships": []
        }
        
        # Node IDs dictionary to track created nodes
        node_ids = {}
        
        # Create source schema node (or reuse existing one)
        source_schema_key = source_schema
        if source_schema_key in node_cache["source_schemas"]:
            source_schema_id = node_cache["source_schemas"][source_schema_key]
            logger.info(f"Reusing existing source schema node: {source_schema_key} (ID: {source_schema_id})")
        else:
            source_schema_node = SourceSchemaNode(
                name=source_schema,
                source_system=source_system_name
            )
            source_schema_id = self.graph.create_node(source_schema_node)
            node_cache["source_schemas"][source_schema_key] = source_schema_id
            logger.info(f"Created new source schema node: {source_schema_key} (ID: {source_schema_id})")
            
            results["nodes"].append({
                "id": source_schema_id,
                "name": source_schema,
                "type": NodeType.SOURCE_SCHEMA
            })
            
        node_ids['source_schema'] = source_schema_id
        
        # Create target schema node (or reuse existing one)
        target_schema_key = target_schema
        if target_schema_key in node_cache["target_schemas"]:
            target_schema_id = node_cache["target_schemas"][target_schema_key]
            logger.info(f"Reusing existing target schema node: {target_schema_key} (ID: {target_schema_id})")
        else:
            target_schema_node = TargetSchemaNode(
                name=target_schema
            )
            target_schema_id = self.graph.create_node(target_schema_node)
            node_cache["target_schemas"][target_schema_key] = target_schema_id
            logger.info(f"Created new target schema node: {target_schema_key} (ID: {target_schema_id})")
            
            results["nodes"].append({
                "id": target_schema_id,
                "name": target_schema,
                "type": NodeType.TARGET_SCHEMA
            })
            
        node_ids['target_schema'] = target_schema_id
        
        # Create source table node (or reuse existing one)
        source_table_key = f"{source_schema}.{source_table}"
        if source_table_key in node_cache["source_tables"]:
            source_table_id = node_cache["source_tables"][source_table_key]
            logger.info(f"Reusing existing source table node: {source_table_key} (ID: {source_table_id})")
        else:
            source_table_node = SourceTableNode(
                name=source_table,
                schema=source_schema,
                description=f"Source table for {target_table}"
            )
            source_table_id = self.graph.create_node(source_table_node)
            node_cache["source_tables"][source_table_key] = source_table_id
            logger.info(f"Created new source table node: {source_table_key} (ID: {source_table_id})")
            
            results["nodes"].append({
                "id": source_table_id,
                "name": source_table,
                "type": NodeType.SOURCE_TABLE
            })
            
            # Create relationship: source_schema CONTAINS source_table
            contains_rel = ContainsRelationship(
                source_id=source_schema_id,
                target_id=source_table_id
            )
            contains_rel_id = self.graph.create_relationship(contains_rel)
            results["relationships"].append({
                "id": contains_rel_id,
                "type": RelationshipType.CONTAINS,
                "source": source_schema,
                "target": source_table
            })
            
        node_ids['source_table'] = source_table_id
        
        # Create target table node (or reuse existing one)
        target_table_key = f"{target_schema}.{target_table}"
        if target_table_key in node_cache["target_tables"]:
            target_table_id = node_cache["target_tables"][target_table_key]
            logger.info(f"Reusing existing target table node: {target_table_key} (ID: {target_table_id})")
        else:
            target_table_node = TargetTableNode(
                name=target_table,
                schema=target_schema,
                description=description,
                entity_type=entity_type,
                collision_code=collision_code
            )
            target_table_id = self.graph.create_node(target_table_node)
            node_cache["target_tables"][target_table_key] = target_table_id
            logger.info(f"Created new target table node: {target_table_key} (ID: {target_table_id})")
            
            results["nodes"].append({
                "id": target_table_id,
                "name": target_table,
                "type": NodeType.TARGET_TABLE
            })
            
            # Create relationship: target_schema CONTAINS target_table
            contains_rel = ContainsRelationship(
                source_id=target_schema_id,
                target_id=target_table_id
            )
            contains_rel_id = self.graph.create_relationship(contains_rel)
            results["relationships"].append({
                "id": contains_rel_id,
                "type": RelationshipType.CONTAINS,
                "source": target_schema,
                "target": target_table
            })
            
        node_ids['target_table'] = target_table_id
        
        # Create relationship: source_table TRANSFORMS_TO target_table
        # Relationship này luôn được tạo mới vì có thể có các thuộc tính khác nhau
        transforms_rel = TransformsToRelationship(
            source_id=source_table_id,
            target_id=target_table_id
        )
        
        # Add properties directly instead of using metadata dict
        if entity_type:
            transforms_rel.properties["entity_type"] = entity_type
        if collision_code:
            transforms_rel.properties["collision_code"] = collision_code
            
        transforms_rel_id = self.graph.create_relationship(transforms_rel)
        results["relationships"].append({
            "id": transforms_rel_id,
            "type": RelationshipType.TRANSFORMS_TO,
            "source": source_table,
            "target": target_table
        })
        
        # Process columns
        columns = dv_config.get('columns', [])
        if not columns:
            logger.warning(f"No columns found in YAML content for {target_table}")
            
        # Track source column nodes by name
        source_column_ids = {}
        target_column_ids = {}
        
        for column in columns:
            if 'target' not in column:
                logger.warning(f"Column missing 'target' field: {column}")
                continue
                
            target_column_name = column['target']
            target_column_dtype = column.get('dtype', 'VARCHAR')
            target_column_key_type = column.get('key_type', None)
            
            # Create target column node (or reuse existing one)
            target_column_key = f"{target_schema}.{target_table}.{target_column_name}"
            if target_column_key in node_cache["target_columns"]:
                target_column_id = node_cache["target_columns"][target_column_key]
                logger.info(f"Reusing existing target column node: {target_column_key} (ID: {target_column_id})")
                target_column_ids[target_column_name] = target_column_id
            else:
                # Process yaml structure to extract description if available
                target_column_description = ''
                if 'description' in column:
                    target_column_description = column['description']
                elif isinstance(source, dict) and 'description' in source:
                    # If column doesn't have description but source does, use that as a fallback
                    target_column_description = source['description']
                    
                target_column_node = TargetColumnNode(
                    name=target_column_name,
                    table=target_table,
                    schema=target_schema,
                    data_type=target_column_dtype,
                    key_type=target_column_key_type,
                    description=target_column_description
                )
                target_column_id = self.graph.create_node(target_column_node)
                node_cache["target_columns"][target_column_key] = target_column_id
                target_column_ids[target_column_name] = target_column_id
                logger.info(f"Created new target column node: {target_column_key} (ID: {target_column_id})")
                
                results["nodes"].append({
                    "id": target_column_id,
                    "name": target_column_name,
                    "type": NodeType.TARGET_COLUMN
                })
                
                # Create relationship: target_table CONTAINS target_column
                contains_rel = ContainsRelationship(
                    source_id=target_table_id,
                    target_id=target_column_id
                )
                # Thêm relationship type vào properties để đảm bảo rõ ràng
                contains_rel.properties["relationship_type"] = "CONTAINS"
                contains_rel_id = self.graph.create_relationship(contains_rel)
                results["relationships"].append({
                    "id": contains_rel_id,
                    "type": RelationshipType.CONTAINS,
                    "source": target_table,
                    "target": target_column_name
                })
            
            # Process source column(s)
            source = column.get('source', None)
            
            # Handle different source structures
            source_column_names = []
            
            if isinstance(source, str):
                # Case: source is a single string
                source_column_names = [source]
            elif isinstance(source, list):
                # Case: source is a list of strings
                source_column_names = [s for s in source if isinstance(s, str)]
            elif isinstance(source, dict) and 'name' in source:
                # Case: source is a dictionary with name key
                source_column_names = [source['name']]
                source_description = source.get('description', '')
                source_dtype = source.get('dtype', 'VARCHAR')
                
                # Create source column with detailed info
                source_column_key = f"{source_schema}.{source_table}.{source_column_names[0]}"
                if source_column_key in node_cache["source_columns"]:
                    source_column_id = node_cache["source_columns"][source_column_key]
                    logger.info(f"Reusing existing source column node: {source_column_key} (ID: {source_column_id})")
                    source_column_ids[source_column_names[0]] = source_column_id
                else:
                    source_column_node = SourceColumnNode(
                        name=source_column_names[0],
                        table=source_table,
                        schema=source_schema,
                        data_type=source_dtype,
                        description=source_description
                    )
                    source_column_id = self.graph.create_node(source_column_node)
                    node_cache["source_columns"][source_column_key] = source_column_id
                    source_column_ids[source_column_names[0]] = source_column_id
                    logger.info(f"Created new source column node: {source_column_key} (ID: {source_column_id})")
                    
                    results["nodes"].append({
                        "id": source_column_id,
                        "name": source_column_names[0],
                        "type": NodeType.SOURCE_COLUMN
                    })
                    
                    # Create relationship: source_table CONTAINS source_column
                    contains_rel = ContainsRelationship(
                        source_id=source_table_id,
                        target_id=source_column_id
                    )
                    # Thêm relationship type vào properties để đảm bảo rõ ràng
                    contains_rel.properties["relationship_type"] = "CONTAINS"
                    contains_rel_id = self.graph.create_relationship(contains_rel)
                    results["relationships"].append({
                        "id": contains_rel_id,
                        "type": RelationshipType.CONTAINS,
                        "source": source_table,
                        "target": source_column_names[0]
                    })
            
            # Create simple source column nodes for any column names not yet created
            for source_column_name in source_column_names:
                source_column_key = f"{source_schema}.{source_table}.{source_column_name}"
                if source_column_key in node_cache["source_columns"]:
                    source_column_id = node_cache["source_columns"][source_column_key]
                    logger.info(f"Reusing existing source column node: {source_column_key} (ID: {source_column_id})")
                    source_column_ids[source_column_name] = source_column_id
                elif source_column_name not in source_column_ids:
                    source_column_node = SourceColumnNode(
                        name=source_column_name,
                        table=source_table,
                        schema=source_schema,
                        data_type="VARCHAR"  # Default data type
                    )
                    source_column_id = self.graph.create_node(source_column_node)
                    node_cache["source_columns"][source_column_key] = source_column_id
                    source_column_ids[source_column_name] = source_column_id
                    logger.info(f"Created new source column node: {source_column_key} (ID: {source_column_id})")
                    
                    results["nodes"].append({
                        "id": source_column_id,
                        "name": source_column_name,
                        "type": NodeType.SOURCE_COLUMN
                    })
                    
                    # Create relationship: source_table CONTAINS source_column
                    contains_rel = ContainsRelationship(
                        source_id=source_table_id,
                        target_id=source_column_id
                    )
                    # Thêm relationship type vào properties để đảm bảo rõ ràng
                    contains_rel.properties["relationship_type"] = "CONTAINS"
                    contains_rel_id = self.graph.create_relationship(contains_rel)
                    results["relationships"].append({
                        "id": contains_rel_id,
                        "type": RelationshipType.CONTAINS,
                        "source": source_table,
                        "target": source_column_name
                    })
                
                # Create relationship: source_column TRANSFORMS_TO target_column
                # Relationship này luôn được tạo mới vì có thể có các thuộc tính khác nhau
                source_column_id = source_column_ids[source_column_name]
                transforms_rel = TransformsToRelationship(
                    source_id=source_column_id,
                    target_id=target_column_id
                )
                
                # Add key_type directly instead of using metadata dict
                if target_column_key_type:
                    transforms_rel.properties["key_type"] = target_column_key_type
                # Thêm relationship type vào properties để đảm bảo rõ ràng
                transforms_rel.properties["relationship_type"] = "TRANSFORMS_TO"
                
                transforms_rel_id = self.graph.create_relationship(transforms_rel)
                results["relationships"].append({
                    "id": transforms_rel_id,
                    "type": RelationshipType.TRANSFORMS_TO,
                    "source": source_column_name,
                    "target": target_column_name
                })
        
        execution_time = time.time() - start_time
        logger.info(f"Detailed Data Vault graph build completed in {execution_time:.2f} seconds")
        
        # Summarize results
        summary = {
            "nodes_count": len(results["nodes"]),
            "relationships_count": len(results["relationships"]),
            "execution_time": execution_time,
            "source_schema": source_schema,
            "source_table": source_table,
            "target_schema": target_schema,
            "target_table": target_table,
            "entity_type": entity_type
        }
        
        return {
            "summary": summary,
            "details": results
        }
        
    def build_detailed_data_vault(self, db: Session, yaml_content: str, source_system_name: str = "Unknown") -> Dict[str, Any]:
        """
        Build detailed Data Vault graph from YAML content with comprehensive nodes and relationships
        
        Args:
            db: SQLAlchemy database session
            yaml_content: YAML content describing the Data Vault component
            source_system_name: Name of the source system
            
        Returns:
            Dict containing summary of nodes and relationships created
        """
        import yaml
        
        start_time = time.time()
        logger.info(f"Building detailed Data Vault graph from YAML content")
        
        # Parse YAML content
        try:
            dv_config = yaml.safe_load(yaml_content)
            if not dv_config or not isinstance(dv_config, dict):
                raise ValueError("Invalid YAML content structure")
        except Exception as e:
            logger.error(f"Error parsing YAML content: {str(e)}")
            raise ValueError(f"Failed to parse YAML content: {str(e)}")
            
        # Extract basic information
        source_schema = dv_config.get('source_schema')
        source_table = dv_config.get('source_table')
        target_schema = dv_config.get('target_schema')
        target_table = dv_config.get('target_table')
        entity_type = dv_config.get('target_entity_type', 'hub')  # Default to hub
        collision_code = dv_config.get('collision_code')
        description = dv_config.get('description', '')
        
        if not all([source_schema, source_table, target_schema, target_table]):
            raise ValueError("Missing required fields in YAML content")
            
        # Prepare result tracking
        results = {
            "nodes": [],
            "relationships": []
        }
        
        # Node IDs dictionary to track created nodes
        node_ids = {}
        
        # Create source schema node
        source_schema_node = SourceSchemaNode(
            name=source_schema,
            source_system=source_system_name
        )
        source_schema_id = self.graph.create_node(source_schema_node)
        node_ids['source_schema'] = source_schema_id
        results["nodes"].append({
            "id": source_schema_id,
            "name": source_schema,
            "type": NodeType.SOURCE_SCHEMA
        })
        
        # Create target schema node
        target_schema_node = TargetSchemaNode(
            name=target_schema
        )
        target_schema_id = self.graph.create_node(target_schema_node)
        node_ids['target_schema'] = target_schema_id
        results["nodes"].append({
            "id": target_schema_id,
            "name": target_schema,
            "type": NodeType.TARGET_SCHEMA
        })
        
        # Create source table node
        source_table_node = SourceTableNode(
            name=source_table,
            schema=source_schema,
            description=f"Source table for {target_table}"
        )
        source_table_id = self.graph.create_node(source_table_node)
        node_ids['source_table'] = source_table_id
        results["nodes"].append({
            "id": source_table_id,
            "name": source_table,
            "type": NodeType.SOURCE_TABLE
        })
        
        # Create relationship: source_schema CONTAINS source_table
        contains_rel = ContainsRelationship(
            source_id=source_schema_id,
            target_id=source_table_id
        )
        contains_rel_id = self.graph.create_relationship(contains_rel)
        results["relationships"].append({
            "id": contains_rel_id,
            "type": RelationshipType.CONTAINS,
            "source": source_schema,
            "target": source_table
        })
        
        # Create target table node
        target_table_node = TargetTableNode(
            name=target_table,
            schema=target_schema,
            description=description,
            entity_type=entity_type,
            collision_code=collision_code
        )
        target_table_id = self.graph.create_node(target_table_node)
        node_ids['target_table'] = target_table_id
        results["nodes"].append({
            "id": target_table_id,
            "name": target_table,
            "type": NodeType.TARGET_TABLE
        })
        
        # Create relationship: target_schema CONTAINS target_table
        contains_rel = ContainsRelationship(
            source_id=target_schema_id,
            target_id=target_table_id
        )
        contains_rel_id = self.graph.create_relationship(contains_rel)
        results["relationships"].append({
            "id": contains_rel_id,
            "type": RelationshipType.CONTAINS,
            "source": target_schema,
            "target": target_table
        })
        
        # Create relationship: source_table TRANSFORMS_TO target_table
        transforms_rel = TransformsToRelationship(
            source_id=source_table_id,
            target_id=target_table_id
        )
        
        # Add properties directly instead of using metadata dict
        if entity_type:
            transforms_rel.properties["entity_type"] = entity_type
        if collision_code:
            transforms_rel.properties["collision_code"] = collision_code
        transforms_rel_id = self.graph.create_relationship(transforms_rel)
        results["relationships"].append({
            "id": transforms_rel_id,
            "type": RelationshipType.TRANSFORMS_TO,
            "source": source_table,
            "target": target_table
        })
        
        # Process columns
        columns = dv_config.get('columns', [])
        if not columns:
            logger.warning(f"No columns found in YAML content for {target_table}")
            
        # Track source column nodes by name
        source_column_ids = {}
        target_column_ids = {}
        
        for column in columns:
            if 'target' not in column:
                logger.warning(f"Column missing 'target' field: {column}")
                continue
                
            target_column_name = column['target']
            target_column_dtype = column.get('dtype', 'VARCHAR')
            target_column_key_type = column.get('key_type', None)
            
            # Create target column node
            target_column_node = TargetColumnNode(
                name=target_column_name,
                table=target_table,
                schema=target_schema,
                data_type=target_column_dtype,
                key_type=target_column_key_type
            )
            target_column_id = self.graph.create_node(target_column_node)
            target_column_ids[target_column_name] = target_column_id
            results["nodes"].append({
                "id": target_column_id,
                "name": target_column_name,
                "type": NodeType.TARGET_COLUMN
            })
            
            # Create relationship: target_table CONTAINS target_column
            contains_rel = ContainsRelationship(
                source_id=target_table_id,
                target_id=target_column_id
            )
            contains_rel_id = self.graph.create_relationship(contains_rel)
            results["relationships"].append({
                "id": contains_rel_id,
                "type": RelationshipType.CONTAINS,
                "source": target_table,
                "target": target_column_name
            })
            
            # Process source column(s)
            source = column.get('source', None)
            
            # Handle different source structures
            source_column_names = []
            
            if isinstance(source, str):
                # Case: source is a single string
                source_column_names = [source]
            elif isinstance(source, list):
                # Case: source is a list of strings
                source_column_names = [s for s in source if isinstance(s, str)]
            elif isinstance(source, dict) and 'name' in source:
                # Case: source is a dictionary with name key
                source_column_names = [source['name']]
                source_description = source.get('description', '')
                source_dtype = source.get('dtype', 'VARCHAR')
                
                # Create source column with detailed info
                if source_column_names[0] not in source_column_ids:
                    source_column_node = SourceColumnNode(
                        name=source_column_names[0],
                        table=source_table,
                        schema=source_schema,
                        data_type=source_dtype,
                        description=source_description
                    )
                    source_column_id = self.graph.create_node(source_column_node)
                    source_column_ids[source_column_names[0]] = source_column_id
                    results["nodes"].append({
                        "id": source_column_id,
                        "name": source_column_names[0],
                        "type": NodeType.SOURCE_COLUMN
                    })
                    
                    # Create relationship: source_table CONTAINS source_column
                    contains_rel = ContainsRelationship(
                        source_id=source_table_id,
                        target_id=source_column_id
                    )
                    contains_rel_id = self.graph.create_relationship(contains_rel)
                    results["relationships"].append({
                        "id": contains_rel_id,
                        "type": RelationshipType.CONTAINS,
                        "source": source_table,
                        "target": source_column_names[0]
                    })
            
            # Create simple source column nodes for any column names not yet created
            for source_column_name in source_column_names:
                if source_column_name not in source_column_ids:
                    source_column_node = SourceColumnNode(
                        name=source_column_name,
                        table=source_table,
                        schema=source_schema,
                        data_type="VARCHAR"  # Default data type
                    )
                    source_column_id = self.graph.create_node(source_column_node)
                    source_column_ids[source_column_name] = source_column_id
                    results["nodes"].append({
                        "id": source_column_id,
                        "name": source_column_name,
                        "type": NodeType.SOURCE_COLUMN
                    })
                    
                    # Create relationship: source_table CONTAINS source_column
                    contains_rel = ContainsRelationship(
                        source_id=source_table_id,
                        target_id=source_column_id
                    )
                    contains_rel_id = self.graph.create_relationship(contains_rel)
                    results["relationships"].append({
                        "id": contains_rel_id,
                        "type": RelationshipType.CONTAINS,
                        "source": source_table,
                        "target": source_column_name
                    })
                
                # Create relationship: source_column TRANSFORMS_TO target_column
                source_column_id = source_column_ids[source_column_name]
                transforms_rel = TransformsToRelationship(
                    source_id=source_column_id,
                    target_id=target_column_id
                )
                
                # Add key_type directly instead of using metadata dict
                if target_column_key_type:
                    transforms_rel.properties["key_type"] = target_column_key_type
                transforms_rel_id = self.graph.create_relationship(transforms_rel)
                results["relationships"].append({
                    "id": transforms_rel_id,
                    "type": RelationshipType.TRANSFORMS_TO,
                    "source": source_column_name,
                    "target": target_column_name
                })
        
        execution_time = time.time() - start_time
        logger.info(f"Detailed Data Vault graph build completed in {execution_time:.2f} seconds")
        
        # Summarize results
        summary = {
            "nodes_count": len(results["nodes"]),
            "relationships_count": len(results["relationships"]),
            "execution_time": execution_time,
            "source_schema": source_schema,
            "source_table": source_table,
            "target_schema": target_schema,
            "target_table": target_table,
            "entity_type": entity_type
        }
        
        return {
            "summary": summary,
            "details": results
        }
