"""
Unit tests for the Graph Connector service.
"""
import pytest
from unittest.mock import patch, Mock
import os

from app.knowledge_graph.services.graph_connector import GraphConnector
from app.knowledge_graph.models.node_models import SourceSystemNode, NodeType
from app.knowledge_graph.models.relationship_models import ContainsRelationship, RelationshipType


# Mock Neo4j connection
@pytest.fixture
def mock_neo4j():
    with patch('neo4j.GraphDatabase') as mock_graph_db:
        # Mock driver
        mock_driver = Mock()
        mock_graph_db.driver.return_value = mock_driver
        
        # Mock session
        mock_session = Mock()
        mock_driver.session.return_value.__enter__.return_value = mock_session
        
        # Mock run
        mock_result = Mock()
        mock_record = Mock()
        mock_record.__getitem__.return_value = "1"
        mock_result.single.return_value = mock_record
        mock_session.run.return_value = mock_result
        
        yield {
            'driver': mock_driver,
            'session': mock_session,
            'result': mock_result,
            'record': mock_record
        }


class TestGraphConnector:
    
    def test_create_node(self, mock_neo4j):
        """Test creating a node"""
        # Arrange
        connector = GraphConnector()
        node = SourceSystemNode(
            name="test_source",
            description="Test source system"
        )
        
        # Act
        node_id = connector.create_node(node)
        
        # Assert
        assert node_id == "1"
        mock_neo4j['session'].run.assert_called_once()
        
        # Check that the query contains the correct label
        call_args = mock_neo4j['session'].run.call_args[0][0]
        assert "SourceSystem" in call_args
    
    def test_create_relationship(self, mock_neo4j):
        """Test creating a relationship"""
        # Arrange
        connector = GraphConnector()
        relationship = ContainsRelationship(
            source_id="1",
            target_id="2"
        )
        
        # Act
        rel_id = connector.create_relationship(relationship)
        
        # Assert
        assert rel_id == "1"
        mock_neo4j['session'].run.assert_called_once()
        
        # Check that the query contains the correct relationship type
        call_args = mock_neo4j['session'].run.call_args[0][0]
        assert "CONTAINS" in call_args
    
    def test_find_nodes_by_properties(self, mock_neo4j):
        """Test finding nodes by properties"""
        # Arrange
        connector = GraphConnector()
        mock_neo4j['result'].single.return_value = None
        mock_neo4j['result'].__iter__.return_value = [
            {"n": {"name": "test"}, "id": "1", "labels": ["SourceSystem"]}
        ]
        
        # Act
        nodes = connector.find_nodes_by_properties(
            node_type=NodeType.SOURCE_SYSTEM,
            properties={"name": "test"}
        )
        
        # Assert
        assert len(nodes) == 1
        assert nodes[0]["id"] == "1"
        mock_neo4j['session'].run.assert_called_once()
    
    def test_execute_cypher(self, mock_neo4j):
        """Test executing a custom Cypher query"""
        # Arrange
        connector = GraphConnector()
        mock_neo4j['result'].single.return_value = None
        mock_neo4j['result'].__iter__.return_value = [
            {"n": {"name": "test"}, "id": "1"}
        ]
        
        # Act
        results = connector.execute_cypher("MATCH (n) RETURN n, id(n) as id")
        
        # Assert
        assert len(results) == 1
        mock_neo4j['session'].run.assert_called_once_with("MATCH (n) RETURN n, id(n) as id")
