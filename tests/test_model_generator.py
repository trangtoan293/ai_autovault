"""
Tests for model generator functionality
"""
import pytest
from unittest.mock import patch, MagicMock
import os
import yaml

from app.services.model_generator import ModelGeneratorService
from app.models.response import ModelConfig


@pytest.fixture
def model_generator_service():
    """Model generator service fixture"""
    service = ModelGeneratorService()
    # Override templates dir for testing
    service.templates_dir = os.path.join(os.path.dirname(__file__), "..", "templates", "models")
    return service


class TestModelGenerator:
    """Test model generator functionality"""
    
    @pytest.mark.asyncio
    async def test_generate_models(self, model_generator_service, test_db, monkeypatch):
        """Test model generation"""
        # Mock metadata service
        mock_metadata = MagicMock()
        mock_metadata.table_name = "test_table"
        mock_metadata.column_name = "id"
        mock_metadata.data_type = "int"
        mock_metadata.is_primary_key = True
        mock_metadata.is_foreign_key = False
        mock_metadata.nullable = False
        
        # Mock database query
        mock_query = MagicMock()
        mock_query.filter.return_value.all.return_value = [mock_metadata]
        monkeypatch.setattr("sqlalchemy.orm.Session.query", lambda *args, **kwargs: mock_query)
        
        # Create config
        config = ModelConfig(
            table_name="test_table",
            model_type="hub",
            use_ai_enhancement=False
        )
        
        # Generate model
        result = await model_generator_service.generate_models(config, test_db["session"])
        
        # Check result
        assert result.table_name == "test_table"
        assert result.model_type == "hub"
        assert "{{" in result.model_yaml  # Template variables should be preserved
        assert "test_table" in result.model_yaml
    
    def test_get_available_templates(self, model_generator_service, monkeypatch):
        """Test listing available templates"""
        # Mock os.listdir
        monkeypatch.setattr("os.listdir", lambda path: ["hub.yml.j2", "link.yml.j2", "satellite.yml.j2"])
        
        # Get templates
        templates = model_generator_service.get_available_templates()
        
        # Check result
        assert templates == ["hub", "link", "satellite"]
    
    @pytest.mark.asyncio
    async def test_preview_model(self, model_generator_service, test_db, monkeypatch):
        """Test model preview"""
        # Mock metadata service
        mock_get_metadata = MagicMock()
        mock_get_metadata.return_value.table_name = "test_table"
        mock_get_metadata.return_value.column_name = "id"
        mock_get_metadata.return_value.data_type = "int"
        monkeypatch.setattr(model_generator_service.metadata_service, "get_metadata", mock_get_metadata)
        
        # Mock generate_models
        mock_generate = MagicMock()
        mock_generate.return_value.model_yaml = "test yaml"
        mock_generate.return_value.table_name = "test_table"
        monkeypatch.setattr(model_generator_service, "generate_models", mock_generate)
        
        # Preview model
        result = await model_generator_service.preview_model(1, "hub", test_db["session"])
        
        # Check result
        assert isinstance(result.model_yaml, str)
        assert result.table_name == "test_table"
    
    @pytest.mark.asyncio
    @patch("openai.ChatCompletion.acreate")
    async def test_enhance_with_ai(self, mock_acreate, model_generator_service, mock_openai_response):
        """Test AI enhancement"""
        # Setup OpenAI API key
        model_generator_service.openai_api_key = "test-key"
        
        # Mock OpenAI API response
        mock_acreate.return_value = mock_openai_response
        
        # Create context
        context = {
            "table_name": "test_table",
            "model_type": "hub",
            "columns": [
                {"column_name": "id", "data_type": "int", "description": "Identifier"}
            ]
        }
        
        # Enhance with AI
        enhanced_context = await model_generator_service._enhance_with_ai(context)
        
        # Check result
        assert "ai_suggestions" in enhanced_context
        assert "business_keys" in enhanced_context["ai_suggestions"]
        assert "model_name" in enhanced_context["ai_suggestions"]
        assert "sensitive_columns" in enhanced_context["ai_suggestions"]
        assert "data_quality_tests" in enhanced_context["ai_suggestions"]
