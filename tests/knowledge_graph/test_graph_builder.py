"""
Unit tests for the Graph Builder service.
"""
import pytest
from unittest.mock import patch, Mock, MagicMock
import os

from app.knowledge_graph.services.graph_builder import GraphBuilder
from app.models.metadata import Metadata
from datetime import datetime


class TestGraphBuilder:
    
    @pytest.fixture
    def mock_graph_connector(self):
        with patch('app.knowledge_graph.services.graph_connector.GraphConnector') as mock_connector:
            connector_instance = Mock()
            mock_connector.return_value = connector_instance
            connector_instance.create_node.return_value = "1"
            connector_instance.create_relationship.return_value = "1"
            connector_instance.find_nodes_by_properties.return_value = [{"id": "1", "name": "test"}]
            connector_instance.execute_cypher.return_value = []
            yield connector_instance
    
    @pytest.fixture
    def mock_metadata_service(self):
        with patch('app.services.metadata_store.MetadataService') as mock_service:
            service_instance = Mock()
            mock_service.return_value = service_instance
            
            # Mock get_session
            mock_db = MagicMock()
            service_instance.get_session.return_value.__enter__.return_value = mock_db
            
            # Mock get_metadata_by_source_system
            mock_metadata = [
                Mock(
                    table_name="test_table",
                    column_name="test_column",
                    data_type="varchar",
                    description="Test column",
                    source_system="test_source",
                    is_primary_key=True,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
            ]
            service_instance.get_metadata_by_source_system.return_value = mock_metadata
            service_instance.get_all_metadata.return_value = mock_metadata
            
            yield service_instance
    
    def test_build_source_metadata_graph(self, mock_graph_connector, mock_metadata_service):
        """Test building source metadata graph"""
        # Arrange
        builder = GraphBuilder()
        mock_db = MagicMock()
        
        # Act
        result = builder.build_source_metadata_graph(mock_db, "test_source")
        
        # Assert
        assert "summary" in result
        assert "details" in result
        assert "source_systems_count" in result["summary"]
        mock_graph_connector.create_node.assert_called()
        mock_graph_connector.create_relationship.assert_called()
        mock_metadata_service.get_metadata_by_source_system.assert_called_once_with(mock_db, "test_source")
