"""
Neo4j graph database connector service.
This service handles the connection and operations with the Neo4j graph database.
"""
import time
from typing import Dict, Any, List, Optional, Union, Tuple
from neo4j import GraphDatabase, Driver, Session, Transaction, Result
from neo4j.exceptions import Neo4jError

from app.core.config import settings
from app.core.logging import logger
from app.knowledge_graph.models.node_models import NodeBase
from app.knowledge_graph.models.relationship_models import RelationshipBase


class GraphConnector:
    """Connector for Neo4j graph database"""
    
    def __init__(self):
        """Initialize connection to Neo4j"""
        self.uri = settings.NEO4J_URI
        self.user = settings.NEO4J_USER
        self.password = settings.NEO4J_PASSWORD
        self.database = settings.NEO4J_DATABASE
        self._driver = None
        self.initialize_connection()
    
    def initialize_connection(self):
        """Initialize the connection to Neo4j"""
        try:
            self._driver = GraphDatabase.driver(
                self.uri, 
                auth=(self.user, self.password)
            )
            # Verify connection
            with self._driver.session(database=self.database) as session:
                result = session.run("RETURN 1 AS x")
                record = result.single()
                if record and record["x"] == 1:
                    logger.info("Successfully connected to Neo4j database")
                    self._initialize_constraints()
                else:
                    logger.error("Failed to verify Neo4j connection")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j database: {str(e)}")
            self._driver = None
            raise
    
    def _initialize_constraints(self):
        """Initialize constraints and indexes for the graph database"""
        constraints = [
            # Unique name constraints for node types
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:SourceSystem) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Table) REQUIRE (n.name, n.schema) IS NODE KEY",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Column) REQUIRE (n.name, n.table, n.schema) IS NODE KEY",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:DataVaultComponent) REQUIRE n.name IS UNIQUE",
            
            # Indexes for better performance
            "CREATE INDEX IF NOT EXISTS FOR (n:SourceSystem) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:Schema) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:Table) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:Column) ON (n.name)",
            "CREATE INDEX IF NOT EXISTS FOR (n:DataVaultComponent) ON (n.component_type)",
        ]
        
        with self._driver.session(database=self.database) as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Neo4jError as e:
                    # Log the error but continue with other constraints
                    logger.warning(f"Failed to create constraint/index: {str(e)}")
    
    @property
    def driver(self) -> Driver:
        """Get the Neo4j driver"""
        if self._driver is None:
            self.initialize_connection()
        return self._driver
    
    def close(self):
        """Close the connection"""
        if self._driver:
            self._driver.close()
            self._driver = None
    
    def create_node(self, node: NodeBase) -> str:
        """
        Create a node in the graph database
        Returns the ID of the created node
        """
        with self.driver.session(database=self.database) as session:
            # Prepare labels
            labels = [node.node_type]
            
            # Prepare properties
            # Exclude certain fields and include relevant ones
            exclude_fields = {"id", "properties", "created_at", "updated_at"}
            props = {}
            
            # Add standard properties
            for key, value in node.dict().items():
                if key not in exclude_fields and value is not None:
                    props[key] = value
            
            # Add custom properties
            for key, value in node.properties.items():
                props[key] = value
            
            # Add timestamps
            props["created_at"] = node.created_at.isoformat()
            props["updated_at"] = node.updated_at.isoformat()
            
            # Không cần kiểm tra trùng lặp vì bây giờ đã sử dụng MERGE
            
            # Sử dụng MERGE cho node với các label dựa trên các thuộc tính cho ràng buộc
            merge_props = {}
            
            # Xác định các thuộc tính dùng để MERGE dựa vào node type
            if node.node_type == 'SourceSystem' and 'name' in props:
                merge_props['name'] = props['name']
            elif node.node_type == 'Schema' and 'name' in props and 'source_system' in props:
                merge_props['name'] = props['name']
                merge_props['source_system'] = props['source_system']
            elif node.node_type == 'Table' and 'name' in props and 'schema' in props:
                merge_props['name'] = props['name']
                merge_props['schema'] = props['schema']
            elif node.node_type == 'Column' and 'name' in props and 'table' in props and 'schema' in props:
                merge_props['name'] = props['name']
                merge_props['table'] = props['table']
                merge_props['schema'] = props['schema']
            elif node.node_type == 'DataVaultComponent' and 'name' in props:
                merge_props['name'] = props['name']
            else:
                # Nếu không có thuộc tính để merge, sử dụng tất cả thuộc tính
                merge_props = props
            
            # Thực hiện MERGE
            try:
                # Đảm bảo updated_at không nằm trong merge_props để tránh trùng lặp
                merge_props_copy = merge_props.copy()
                if 'updated_at' in merge_props_copy:
                    del merge_props_copy['updated_at']
                    
                result = session.run(
                    f"""
                    MERGE (n:{':'.join(labels)} {{{', '.join([f'{k}: ${k}' for k in merge_props_copy])}}}) 
                    ON CREATE SET n += $all_props
                    ON MATCH SET n.updated_at = $updated_at
                    RETURN id(n) as id
                    """,
                    **merge_props_copy,
                    all_props=props,
                    updated_at=props['updated_at']
                )
            except Neo4jError as e:
                logger.error(f"Error creating node {node.node_type}: {str(e)}")
                raise Exception(f"Failed to create {node.node_type} node: {str(e)}")
            
            record = result.single()
            if record:
                return str(record["id"])
            else:
                raise Exception("Failed to create node")
    
    def create_relationship(self, relationship: RelationshipBase) -> str:
        """
        Create a relationship in the graph database
        Returns the ID of the created relationship
        """
        with self.driver.session(database=self.database) as session:
            # Prepare properties
            exclude_fields = {"id", "relationship_type", "source_id", "target_id", "properties", "created_at", "updated_at"}
            props = {}
            
            # Add standard properties
            for key, value in relationship.dict().items():
                if key not in exclude_fields and value is not None:
                    props[key] = value
            
            # Add custom properties
            for key, value in relationship.properties.items():
                # Kiểm tra nếu value là một từ điển (dict), cần phân tách thành các thuộc tính riêng lẻ
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        props[f"{key}_{sub_key}"] = sub_value
                else:
                    props[key] = value
            
            # Add timestamps
            props["created_at"] = relationship.created_at.isoformat()
            props["updated_at"] = relationship.updated_at.isoformat()
            
            # Extract relationship type value to handle enum properly
            rel_type = relationship.relationship_type
            
            # Khi làm việc với enum, cần lấy giá trị thật sự của enum chứ không phải tên
            if hasattr(rel_type, 'value'):
                # Đây là một Enum object
                rel_type = rel_type.value
            else:
                # Đảm bảo chuyển thành chuỗi
                rel_type = str(rel_type)
            
            # Trong trường hợp giá trị chuỗi chứa dấu chấm, lấy phần sau dấu chấm cuối cùng
            if '.' in rel_type:
                rel_type = rel_type.split('.')[-1]
                
            logger.info(f"Creating relationship of type: {rel_type}")
            
            # Sử dụng MERGE thay vì CREATE để tự động xử lý trùng lặp
            try:
                # Log thêm thông tin để debug
                logger.info(f"Relationship object: {relationship}")
                logger.info(f"Relationship type: {rel_type}, Type: {type(rel_type)}")
                logger.info(f"Source ID: {relationship.source_id}, Target ID: {relationship.target_id}")
                
                cypher_query = f"""
                    MATCH (source), (target)
                    WHERE id(source) = $source_id AND id(target) = $target_id
                    MERGE (source)-[r:{rel_type}]->(target)
                    ON CREATE SET r += $props, r.created_at = $created_at, r.updated_at = $updated_at
                    ON MATCH SET r.updated_at = $updated_at
                    RETURN id(r) as id
                    """
                
                logger.info(f"Executing Cypher: {cypher_query}")
                
                result = session.run(
                    cypher_query,
                    source_id=int(relationship.source_id),
                    target_id=int(relationship.target_id),
                    props=props,
                    created_at=props["created_at"],
                    updated_at=props["updated_at"]
                )
            except Neo4jError as e:
                logger.error(f"Neo4j Error: {e.code} - {e.message}")
                # Kiểm tra xem có phải lỗi cú pháp với dấu chấm không
                if 'Invalid input \'.\'' in str(e):
                    logger.error("Dấu chấm được phát hiện trong relationship type. Đây có thể là vấn đề với enum")
                    # Thử lại với cách tiếp cận khác
                    rel_type_fixed = 'CONTAINS'  # Sử dụng giá trị cứng để khắc phục vấn đề
                    logger.info(f"Thử lại với relationship type cố định: {rel_type_fixed}")
                    
                    try:
                        result = session.run(
                            f"""
                            MATCH (source), (target)
                            WHERE id(source) = $source_id AND id(target) = $target_id
                            MERGE (source)-[r:{rel_type_fixed}]->(target)
                            ON CREATE SET r += $props, r.created_at = $created_at, r.updated_at = $updated_at
                            ON MATCH SET r.updated_at = $updated_at
                            RETURN id(r) as id
                            """,
                            source_id=int(relationship.source_id),
                            target_id=int(relationship.target_id),
                            props=props,
                            created_at=props["created_at"],
                            updated_at=props["updated_at"]
                        )
                        
                        record = result.single()
                        if record:
                            return str(record["id"])
                    except Exception as nested_e:
                        logger.error(f"Nested error when trying fixed relationship type: {str(nested_e)}")
                
                raise Exception(f"Failed to create relationship between nodes {relationship.source_id} and {relationship.target_id}: {str(e)}")
            
            record = result.single()
            if record:
                return str(record["id"])
            else:
                raise Exception(f"Failed to create relationship between nodes {relationship.source_id} and {relationship.target_id}")
    
    def find_node_by_id(self, node_id: str) -> Optional[Dict[str, Any]]:
        """
        Find a node by ID
        """
        with self.driver.session(database=self.database) as session:
            result = session.run(
                """
                MATCH (n)
                WHERE id(n) = $node_id
                RETURN n, labels(n) as labels, id(n) as id
                """,
                node_id=int(node_id)
            )
            
            record = result.single()
            if record:
                node = dict(record["n"])
                node["id"] = str(record["id"])
                node["labels"] = record["labels"]
                return node
            else:
                return None
    
    def find_relationship_by_id(self, relationship_id: str) -> Optional[Dict[str, Any]]:
        """
        Find a relationship by ID
        """
        with self.driver.session(database=self.database) as session:
            result = session.run(
                """
                MATCH ()-[r]->()
                WHERE id(r) = $relationship_id
                RETURN r, type(r) as type, id(r) as id, id(startNode(r)) as source_id, id(endNode(r)) as target_id
                """,
                relationship_id=int(relationship_id)
            )
            
            record = result.single()
            if record:
                rel = dict(record["r"])
                rel["id"] = str(record["id"])
                rel["type"] = record["type"]
                rel["source_id"] = str(record["source_id"])
                rel["target_id"] = str(record["target_id"])
                return rel
            else:
                return None
    
    def find_nodes_by_properties(
        self, 
        node_type: str, 
        properties: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Find nodes by properties
        """
        if not properties:
            return []
        
        with self.driver.session(database=self.database) as session:
            # Construct WHERE clause dynamically
            where_clauses = []
            params = {}
            
            for idx, (key, value) in enumerate(properties.items()):
                param_name = f"prop{idx}"
                where_clauses.append(f"n.{key} = ${param_name}")
                params[param_name] = value
            
            where_clause = " AND ".join(where_clauses)
            
            # Execute query
            result = session.run(
                f"""
                MATCH (n:{node_type})
                WHERE {where_clause}
                RETURN n, labels(n) as labels, id(n) as id
                """,
                **params
            )
            
            nodes = []
            for record in result:
                node = dict(record["n"])
                node["id"] = str(record["id"])
                node["labels"] = record["labels"]
                nodes.append(node)
            
            return nodes
    
    def execute_cypher(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Execute a custom Cypher query
        """
        if params is None:
            params = {}
        
        with self.driver.session(database=self.database) as session:
            result = session.run(query, **params)
            
            # Convert records to dictionaries
            records = []
            for record in result:
                record_dict = {}
                for key, value in record.items():
                    # Handle Neo4j node, relationship, and path types
                    if hasattr(value, "id") and callable(value.id):
                        # This is a Neo4j node or relationship
                        record_dict[key] = {
                            "id": str(value.id),
                            "properties": dict(value)
                        }
                        
                        # Add additional info for nodes
                        if hasattr(value, "labels") and callable(value.labels):
                            record_dict[key]["labels"] = value.labels()
                        
                        # Add additional info for relationships
                        if hasattr(value, "type") and callable(value.type):
                            record_dict[key]["type"] = value.type
                            record_dict[key]["start_node_id"] = str(value.start_node.id)
                            record_dict[key]["end_node_id"] = str(value.end_node.id)
                    elif hasattr(value, "nodes") and callable(value.nodes) and hasattr(value, "relationships") and callable(value.relationships):
                        # This is a Neo4j path
                        path_nodes = []
                        for node in value.nodes:
                            path_nodes.append({
                                "id": str(node.id),
                                "labels": node.labels(),
                                "properties": dict(node)
                            })
                        
                        path_relationships = []
                        for rel in value.relationships:
                            path_relationships.append({
                                "id": str(rel.id),
                                "type": rel.type,
                                "start_node_id": str(rel.start_node.id),
                                "end_node_id": str(rel.end_node.id),
                                "properties": dict(rel)
                            })
                        
                        record_dict[key] = {
                            "nodes": path_nodes,
                            "relationships": path_relationships
                        }
                    else:
                        # This is a primitive type
                        record_dict[key] = value
                
                records.append(record_dict)
            
            return records
    
    def clear_database(self) -> None:
        """
        Clear all data from the database
        WARNING: This will delete all nodes and relationships
        """
        with self.driver.session(database=self.database) as session:
            session.run("MATCH (n) DETACH DELETE n")
