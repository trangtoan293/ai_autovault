"""
Tests for data ingestion functionality
"""
import os
import pytest
from fastapi import UploadFile
import pandas as pd

from app.services.data_ingestion import DataIngestionService


@pytest.fixture
def data_ingestion_service():
    """Data ingestion service fixture"""
    return DataIngestionService()


class TestDataIngestion:
    """Test data ingestion functionality"""
    
    @pytest.mark.asyncio
    async def test_process_csv(self, data_ingestion_service, sample_csv_file):
        """Test CSV processing"""
        # Create UploadFile from sample file
        with open(sample_csv_file, 'rb') as f:
            upload_file = UploadFile(filename=os.path.basename(sample_csv_file), file=f)
            
            # Process file
            df = await data_ingestion_service.process_file(upload_file)
            
            # Check if DataFrame was created correctly
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3
            assert list(df.columns) == ["id", "name", "value", "created_at"]
            assert df["id"].tolist() == [1, 2, 3]
    
    @pytest.mark.asyncio
    async def test_process_excel(self, data_ingestion_service, sample_excel_file):
        """Test Excel processing"""
        # Create UploadFile from sample file
        with open(sample_excel_file, 'rb') as f:
            upload_file = UploadFile(filename=os.path.basename(sample_excel_file), file=f)
            
            # Process file
            df = await data_ingestion_service.process_file(upload_file)
            
            # Check if DataFrame was created correctly
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3
            assert list(df.columns) == ["id", "name", "value", "created_at"]
            assert df["id"].tolist() == [1, 2, 3]
    
    def test_extract_metadata(self, data_ingestion_service):
        """Test metadata extraction"""
        # Create sample DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Test 1", "Test 2", "Test 3"],
            "value": [100, 200, 300],
            "nullable_col": [None, 2, 3]
        })
        
        # Extract metadata
        metadata = data_ingestion_service.extract_metadata(df)
        
        # Check if metadata was extracted correctly
        assert len(metadata) == 4  # One entry per column
        
        # Check id column metadata
        id_meta = [m for m in metadata if m["column_name"] == "id"][0]
        assert id_meta["data_type"] == "int64"
        assert id_meta["nullable"] is False
        assert id_meta["unique_values"] == 3
        
        # Check nullable column metadata
        null_meta = [m for m in metadata if m["column_name"] == "nullable_col"][0]
        assert null_meta["nullable"] is True
    
    def test_validate_data_quality(self, data_ingestion_service):
        """Test data quality validation"""
        # Create sample DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Test 1", "Test 2", "Test 3"],
            "value": [100, 200, 300],
            "nullable_col": [None, 2, 3]
        })
        
        # Validate data quality
        quality_report = data_ingestion_service.validate_data_quality(df)
        
        # Check if quality report was generated correctly
        assert quality_report["row_count"] == 3
        assert quality_report["column_count"] == 4
        assert quality_report["duplicate_rows"] == 0
        assert quality_report["missing_values"]["nullable_col"] == 1
        
        # Check column stats
        assert "id" in quality_report["column_stats"]
        assert quality_report["column_stats"]["id"]["unique_count"] == 3
        assert quality_report["column_stats"]["id"]["missing_count"] == 0
        
        # Check numeric stats
        assert "min" in quality_report["column_stats"]["value"]
        assert quality_report["column_stats"]["value"]["min"] == 100
        assert quality_report["column_stats"]["value"]["max"] == 300
