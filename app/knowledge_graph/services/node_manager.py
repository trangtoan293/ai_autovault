"""
Node Manager Service.
Manages the creation and retrieval of nodes with uniqueness guarantees.
"""
from app.core.logging import logger
from app.knowledge_graph.services.graph_connector import GraphConnector
from app.knowledge_graph.models.node_models import (
    SourceSystemNode, SourceSchemaNode, SourceTableNode, SourceColumnNode,
    TargetSchemaNode, TargetTableNode, TargetColumnNode
)

class NodeManagerService:
    """Service to manage node creation and retrieval with uniqueness guarantees"""
    
    def __init__(self):
        """Initialize the node manager"""
        self.graph = GraphConnector()
    
    # Source nodes methods
    
    def get_or_create_source_system_node(self, name, description=None):
        """
        Get or create a SourceSystemNode with given name
        
        Args:
            name: Name of the source system
            description: Optional description
            
        Returns:
            str: Node ID (existing or new)
        """
        # Find existing node
        existing = self.graph.execute_cypher(
            """
            MATCH (n:SourceSystemNode {name: $name})
            RETURN id(n) as id
            """,
            params={"name": name}
        )
        
        if existing:
            logger.info(f"Reusing existing source system node: {name} (ID: {existing[0]['id']})")
            return existing[0]["id"]
        
        # Create new node
        node = SourceSystemNode(
            name=name,
            description=description or f"Source system: {name}"
        )
        node_id = self.graph.create_node(node)
        logger.info(f"Created new source system node: {name} (ID: {node_id})")
        
        return node_id
    
    def get_or_create_source_schema_node(self, name, source_system):
        """
        Get or create a SourceSchemaNode with given name
        
        Args:
            name: Name of the schema
            source_system: Name of the source system
            
        Returns:
            str: Node ID (existing or new)
        """
        # Find existing node
        existing = self.graph.execute_cypher(
            """
            MATCH (n:SourceSchemaNode {name: $name})
            RETURN id(n) as id
            """,
            params={"name": name}
        )
        
        if existing:
            logger.info(f"Reusing existing source schema node: {name} (ID: {existing[0]['id']})")
            return existing[0]["id"]
        
        # Create new node
        node = SourceSchemaNode(
            name=name,
            source_system=source_system
        )
        node_id = self.graph.create_node(node)
        logger.info(f"Created new source schema node: {name} (ID: {node_id})")
        
        return node_id
    
    def get_or_create_source_table_node(self, name, schema, description=None):
        """
        Get or create a SourceTableNode with given name and schema
        
        Args:
            name: Name of the table
            schema: Name of the schema
            description: Optional description
            
        Returns:
            str: Node ID (existing or new)
        """
        # Find existing node
        existing = self.graph.execute_cypher(
            """
            MATCH (n:SourceTableNode {name: $name, schema: $schema})
            RETURN id(n) as id
            """,
            params={"name": name, "schema": schema}
        )
        
        if existing:
            logger.info(f"Reusing existing source table node: {schema}.{name} (ID: {existing[0]['id']})")
            return existing[0]["id"]
        
        # Create new node
        node = SourceTableNode(
            name=name,
            schema=schema,
            description=description
        )
        node_id = self.graph.create_node(node)
        logger.info(f"Created new source table node: {schema}.{name} (ID: {node_id})")
        
        return node_id
    
    def get_or_create_source_column_node(self, name, table, schema, **kwargs):
        """
        Get or create a SourceColumnNode with given name, table and schema
        
        Args:
            name: Name of the column
            table: Name of the table
            schema: Name of the schema
            **kwargs: Additional properties for the column
            
        Returns:
            str: Node ID (existing or new)
        """
        # Find existing node
        existing = self.graph.execute_cypher(
            """
            MATCH (n:SourceColumnNode {name: $name, table: $table, schema: $schema})
            RETURN id(n) as id
            """,
            params={"name": name, "table": table, "schema": schema}
        )
        
        if existing:
            logger.info(f"Reusing existing source column node: {schema}.{table}.{name} (ID: {existing[0]['id']})")
            return existing[0]["id"]
        
        # Create new node
        node = SourceColumnNode(
            name=name,
            table=table,
            schema=schema,
            **kwargs
        )
        node_id = self.graph.create_node(node)
        logger.info(f"Created new source column node: {schema}.{table}.{name} (ID: {node_id})")
        
        return node_id
    
    # Target nodes methods
    
    def get_or_create_target_schema_node(self, name):
        """
        Get or create a TargetSchemaNode with given name
        
        Args:
            name: Name of the schema
            
        Returns:
            str: Node ID (existing or new)
        """
        # Find existing node
        existing = self.graph.execute_cypher(
            """
            MATCH (n:TargetSchemaNode {name: $name})
            RETURN id(n) as id
            """,
            params={"name": name}
        )
        
        if existing:
            logger.info(f"Reusing existing target schema node: {name} (ID: {existing[0]['id']})")
            return existing[0]["id"]
        
        # Create new node
        node = TargetSchemaNode(
            name=name
        )
        node_id = self.graph.create_node(node)
        logger.info(f"Created new target schema node: {name} (ID: {node_id})")
        
        return node_id
    
    def get_or_create_target_table_node(self, name, schema, description=None, entity_type=None, collision_code=None):
        """
        Get or create a TargetTableNode with given name and schema
        
        Args:
            name: Name of the table
            schema: Name of the schema
            description: Optional description
            entity_type: Optional entity type
            collision_code: Optional collision code
            
        Returns:
            str: Node ID (existing or new)
        """
        # Find existing node
        existing = self.graph.execute_cypher(
            """
            MATCH (n:TargetTableNode {name: $name, schema: $schema})
            RETURN id(n) as id
            """,
            params={"name": name, "schema": schema}
        )
        
        if existing:
            logger.info(f"Reusing existing target table node: {schema}.{name} (ID: {existing[0]['id']})")
            return existing[0]["id"]
        
        # Create new node
        node = TargetTableNode(
            name=name,
            schema=schema,
            description=description,
            entity_type=entity_type,
            collision_code=collision_code
        )
        node_id = self.graph.create_node(node)
        logger.info(f"Created new target table node: {schema}.{name} (ID: {node_id})")
        
        return node_id
    
    def get_or_create_target_column_node(self, name, table, schema, data_type=None, key_type=None, description=None):
        """
        Get or create a TargetColumnNode with given name, table and schema
        
        Args:
            name: Name of the column
            table: Name of the table
            schema: Name of the schema
            data_type: Optional data type
            key_type: Optional key type
            description: Optional description
            
        Returns:
            str: Node ID (existing or new)
        """
        # Find existing node
        existing = self.graph.execute_cypher(
            """
            MATCH (n:TargetColumnNode {name: $name, table: $table, schema: $schema})
            RETURN id(n) as id
            """,
            params={"name": name, "table": table, "schema": schema}
        )
        
        if existing:
            logger.info(f"Reusing existing target column node: {schema}.{table}.{name} (ID: {existing[0]['id']})")
            return existing[0]["id"]
        
        # Create new node
        node = TargetColumnNode(
            name=name,
            table=table,
            schema=schema,
            data_type=data_type,
            key_type=key_type,
            description=description
        )
        node_id = self.graph.create_node(node)
        logger.info(f"Created new target column node: {schema}.{table}.{name} (ID: {node_id})")
        
        return node_id
