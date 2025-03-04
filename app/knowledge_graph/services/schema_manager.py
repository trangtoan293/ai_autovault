"""
Neo4j Schema Manager.
Handles schema setup for Neo4j including constraints and indexes.
"""
from app.core.logging import logger
from app.knowledge_graph.services.graph_connector import GraphConnector

class SchemaManager:
    """Manages Neo4j schema configuration"""
    
    def __init__(self):
        """Initialize the schema manager"""
        self.graph = GraphConnector()
    
    def setup_schema(self):
        """Setup all necessary schema configurations including constraints and indexes"""
        logger.info("Setting up Neo4j schema configurations")
        
        # Setup Source node constraints
        self._setup_source_constraints()
        
        # Setup Target node constraints
        self._setup_target_constraints()
        
        logger.info("Schema configuration completed")
    
    def _setup_source_constraints(self):
        """Setup constraints for Source nodes"""
        try:
            # Check if constraints already exist
            constraints = self.graph.execute_cypher(
                """
                SHOW CONSTRAINTS
                """
            )
            
            existing_constraints = [c.get('name', '') for c in constraints]
            
            # Constraint for SourceSchemaNode
            if 'unique_source_schema_name' not in existing_constraints:
                self.graph.execute_cypher(
                    """
                    CREATE CONSTRAINT unique_source_schema_name IF NOT EXISTS
                    FOR (n:SourceSchemaNode) REQUIRE n.name IS UNIQUE
                    """
                )
                logger.info("Created constraint: unique_source_schema_name")
            
            # Constraint for SourceTableNode
            if 'unique_source_table_name' not in existing_constraints:
                self.graph.execute_cypher(
                    """
                    CREATE CONSTRAINT unique_source_table_name IF NOT EXISTS
                    FOR (n:SourceTableNode) REQUIRE (n.schema, n.name) IS UNIQUE
                    """
                )
                logger.info("Created constraint: unique_source_table_name")
            
            # Constraint for SourceColumnNode
            if 'unique_source_column_name' not in existing_constraints:
                self.graph.execute_cypher(
                    """
                    CREATE CONSTRAINT unique_source_column_name IF NOT EXISTS
                    FOR (n:SourceColumnNode) REQUIRE (n.schema, n.table, n.name) IS UNIQUE
                    """
                )
                logger.info("Created constraint: unique_source_column_name")
        
        except Exception as e:
            logger.error(f"Error setting up Source constraints: {str(e)}")
            raise
    
    def _setup_target_constraints(self):
        """Setup constraints for Target nodes"""
        try:
            # Check if constraints already exist
            constraints = self.graph.execute_cypher(
                """
                SHOW CONSTRAINTS
                """
            )
            
            existing_constraints = [c.get('name', '') for c in constraints]
            
            # Constraint for TargetSchemaNode
            if 'unique_target_schema_name' not in existing_constraints:
                self.graph.execute_cypher(
                    """
                    CREATE CONSTRAINT unique_target_schema_name IF NOT EXISTS
                    FOR (n:TargetSchemaNode) REQUIRE n.name IS UNIQUE
                    """
                )
                logger.info("Created constraint: unique_target_schema_name")
            
            # Constraint for TargetTableNode
            if 'unique_target_table_name' not in existing_constraints:
                self.graph.execute_cypher(
                    """
                    CREATE CONSTRAINT unique_target_table_name IF NOT EXISTS
                    FOR (n:TargetTableNode) REQUIRE (n.schema, n.name) IS UNIQUE
                    """
                )
                logger.info("Created constraint: unique_target_table_name")
            
            # Constraint for TargetColumnNode
            if 'unique_target_column_name' not in existing_constraints:
                self.graph.execute_cypher(
                    """
                    CREATE CONSTRAINT unique_target_column_name IF NOT EXISTS
                    FOR (n:TargetColumnNode) REQUIRE (n.schema, n.table, n.name) IS UNIQUE
                    """
                )
                logger.info("Created constraint: unique_target_column_name")
        
        except Exception as e:
            logger.error(f"Error setting up Target constraints: {str(e)}")
            raise
