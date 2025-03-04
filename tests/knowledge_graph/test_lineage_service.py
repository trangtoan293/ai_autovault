"""
Unit tests for the Lineage Service.
"""
import pytest
from unittest.mock import patch, Mock
import os

from app.knowledge_graph.services.lineage_service import LineageService


class TestLineageService:
    
    @pytest.fixture
    def mock_graph_connector(self):
        with patch('app.knowledge_graph.services.graph_connector.GraphConnector') as mock_connector:
            connector_instance = Mock()
            mock_connector.return_value = connector_instance
            
            # Mock find_nodes_by_properties
            connector_instance.find_nodes_by_properties.return_value = [
                {"id": "1", "name": "test_table", "schema": "test_schema"}
            ]
            
            # Mock execute_cypher
            mock_node1 = {"id": "1", "name": "test_table", "schema": "test_schema", "properties": {}}
            mock_node2 = {"id": "2", "name": "source_table", "schema": "test_schema", "properties": {}}
            mock_rel = {"id": "3", "type": "SOURCE_OF", "start_node_id": "2", "end_node_id": "1", "properties": {}}
            
            connector_instance.execute_cypher.return_value = [
                {
                    "path": {
                        "nodes": [mock_node1, mock_node2],
                        "relationships": [mock_rel]
                    }
                }
            ]
            
            yield connector_instance
    
    def test_get_table_lineage(self, mock_graph_connector):
        """Test getting table lineage"""
        # Arrange
        service = LineageService()
        
        # Act
        result = service.get_table_lineage("test_table", "test_schema", "both")
        
        # Assert
        assert "message" in result
        assert "table_name" in result
        assert "schema_name" in result
        assert "nodes" in result
        assert "relationships" in result
        mock_graph_connector.find_nodes_by_properties.assert_called_once()
        mock_graph_connector.execute_cypher.assert_called_once()
    
    def test_get_column_lineage(self, mock_graph_connector):
        """Test getting column lineage"""
        # Arrange
        service = LineageService()
        
        # Act
        result = service.get_column_lineage("test_table", "test_column", "test_schema", "both")
        
        # Assert
        assert "message" in result
        assert "table_name" in result
        assert "column_name" in result
        assert "schema_name" in result
        assert "nodes" in result
        assert "relationships" in result
        mock_graph_connector.find_nodes_by_properties.assert_called_once()
        mock_graph_connector.execute_cypher.assert_called_once()
