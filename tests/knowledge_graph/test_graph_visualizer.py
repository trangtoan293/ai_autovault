"""
Unit tests for the Graph Visualizer.
"""
import pytest
from unittest.mock import patch, Mock, MagicMock
import os
import tempfile
from pathlib import Path

from app.knowledge_graph.utils.graph_visualizer import GraphVisualizer


class TestGraphVisualizer:
    
    @pytest.fixture
    def mock_graph_connector(self):
        with patch('app.knowledge_graph.services.graph_connector.GraphConnector') as mock_connector:
            connector_instance = Mock()
            mock_connector.return_value = connector_instance
            
            # Mock execute_cypher to return test data
            node_result = [
                {"n": {"name": "test_table"}, "id": "1", "labels": ["Table"]},
                {"n": {"name": "source_table"}, "id": "2", "labels": ["Table"]}
            ]
            
            rel_result = [
                {
                    "r": {"property": "test"}, 
                    "type": "CONTAINS", 
                    "source_id": "1", 
                    "target_id": "2", 
                    "id": "3"
                }
            ]
            
            # Use side_effect to return different values based on the query
            def mock_execute_cypher(query, params=None):
                if "MATCH (n)" in query:
                    return node_result
                elif "MATCH ()-[r]->()" in query:
                    return rel_result
                else:
                    return []
            
            connector_instance.execute_cypher.side_effect = mock_execute_cypher
            
            yield connector_instance
    
    def test_generate_d3_visualization(self, mock_graph_connector):
        """Test generating D3.js visualization"""
        # Arrange
        visualizer = GraphVisualizer()
        
        # Create temporary file for output
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Act
            result = visualizer.generate_d3_visualization(
                node_query="MATCH (n) RETURN n, labels(n) as labels, id(n) as id",
                relationship_query="MATCH ()-[r]->() RETURN r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id",
                output_file=temp_path
            )
            
            # Assert
            assert "data" in result
            assert "html" in result
            assert "file_path" in result
            assert result["file_path"] == temp_path
            assert os.path.exists(temp_path)
            
            # Check data structure
            assert "nodes" in result["data"]
            assert "links" in result["data"]
            assert len(result["data"]["nodes"]) == 2
            assert len(result["data"]["links"]) == 1
            
            # Check HTML content
            with open(temp_path, "r") as f:
                content = f.read()
                assert "<!DOCTYPE html>" in content
                assert "<title>Knowledge Graph Visualization</title>" in content
                assert "d3.js" in content.lower()
        
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_generate_mermaid_diagram(self, mock_graph_connector):
        """Test generating Mermaid diagram"""
        # Arrange
        visualizer = GraphVisualizer()
        
        # Create temporary file for output
        with tempfile.NamedTemporaryFile(suffix=".md", delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Act
            result = visualizer.generate_mermaid_diagram(
                node_query="MATCH (n) RETURN n, labels(n) as labels, id(n) as id",
                relationship_query="MATCH ()-[r]->() RETURN r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id",
                diagram_type="flowchart",
                output_file=temp_path
            )
            
            # Assert
            assert "data" in result
            assert "file_path" in result
            assert result["file_path"] == temp_path
            assert os.path.exists(temp_path)
            
            # Check Mermaid content
            with open(temp_path, "r") as f:
                content = f.read()
                assert content.startswith("flowchart")
                assert "test_table" in content
                assert "source_table" in content
        
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)
