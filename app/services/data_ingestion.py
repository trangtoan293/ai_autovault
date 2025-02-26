"""
CSV/Excel processing
"""
import io
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from fastapi import UploadFile, HTTPException

from app.core.logging import logger
from app.utils.file_utils import get_file_extension


class DataIngestionService:
    """Service for ingesting and processing CSV/Excel files"""
    
    async def process_file(self, file: UploadFile) -> pd.DataFrame:
        """
        Process uploaded file (CSV or Excel) and convert to DataFrame
        """
        file_ext = get_file_extension(file.filename)
        content = await file.read()
        
        try:
            if file_ext.lower() == "csv":
                df = await self._process_csv(content)
            elif file_ext.lower() in ["xlsx", "xls"]:
                df = await self._process_excel(content)
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
                
            # Validate metadata structure
            self.validate_metadata_structure(df)
            return df
        except ValueError as e:
            logger.error(f"Validation error in file {file.filename}: {str(e)}")
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error(f"Error processing file {file.filename}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
    
    async def _process_csv(self, content: bytes) -> pd.DataFrame:
        """
        Process CSV file content
        """
        try:
            # Try different encodings
            for encoding in ["utf-8", "latin1", "iso-8859-1"]:
                try:
                    df = pd.read_csv(io.BytesIO(content), encoding=encoding)
                    return df
                except UnicodeDecodeError:
                    continue
            
            # If all encodings fail
            raise ValueError("Unable to decode CSV file with supported encodings")
        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")
            raise
    
    async def _process_excel(self, content: bytes) -> pd.DataFrame:
        """
        Process Excel file content
        """
        try:
            df = pd.read_excel(io.BytesIO(content))
            return df
        except Exception as e:
            logger.error(f"Error processing Excel file: {str(e)}")
            raise
    
    def extract_metadata(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Extract metadata from DataFrame that contains schema information
        """
        metadata = []
        
        # Iterate through each row of the DataFrame
        for _, row in df.iterrows():
            column_metadata = {
                "schema_name": row["schema_name"],
                "table_name": row["table_name"],
                "column_name": row["column_name"],
                "data_type": row["column_data_type"],
                "description": row["column_description"],
                "table_description": row["table_description"],
                "nullable": True,  # Default, can be overridden if available in the file
                "unique_values": None,
                "sample_values": []
            }
            metadata.append(column_metadata)
        
        return metadata
    
    def validate_metadata_structure(self, df: pd.DataFrame) -> bool:
        """
        Validate that the DataFrame contains all required metadata fields
        and that they have valid values
        """
        required_columns = [
            'schema_name', 'table_name', 'column_name', 
            'column_data_type', 'table_description', 'column_description'
        ]
        
        # Check if all required columns are present
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required metadata columns: {', '.join(missing_columns)}")
        
        # Check for null values in required fields
        for column in ['schema_name', 'table_name', 'column_name', 'column_data_type']:
            if df[column].isnull().any():
                null_rows = df[df[column].isnull()].index.tolist()
                raise ValueError(f"Column '{column}' contains null values at rows: {null_rows}")
        
        # Validate table names (cannot be empty strings)
        if (df['table_name'].str.strip() == '').any():
            empty_rows = df[df['table_name'].str.strip() == ''].index.tolist()
            raise ValueError(f"Empty table names found at rows: {empty_rows}")
        
        # Validate column names (cannot be empty strings)
        if (df['column_name'].str.strip() == '').any():
            empty_rows = df[df['column_name'].str.strip() == ''].index.tolist()
            raise ValueError(f"Empty column names found at rows: {empty_rows}")
        
        return True
        
    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform data quality validation on DataFrame
        """
        quality_report = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "missing_values": df.isnull().sum().to_dict(),
            "duplicate_rows": df.duplicated().sum(),
            "column_stats": {}
        }
        
        # Generate statistics for each column
        for column in df.columns:
            col_type = df[column].dtype
            stats = {
                "dtype": str(col_type),
                "unique_count": df[column].nunique(),
                "missing_count": df[column].isnull().sum(),
                "missing_percentage": (df[column].isnull().sum() / len(df) * 100) if len(df) > 0 else 0,
            }
            
            # Add numeric statistics if applicable
            if pd.api.types.is_numeric_dtype(col_type):
                stats.update({
                    "min": df[column].min() if not df[column].empty else None,
                    "max": df[column].max() if not df[column].empty else None, 
                    "mean": df[column].mean() if not df[column].empty else None,
                    "median": df[column].median() if not df[column].empty else None,
                    "std": df[column].std() if not df[column].empty else None,
                })
                
            quality_report["column_stats"][column] = stats
            
        return quality_report
