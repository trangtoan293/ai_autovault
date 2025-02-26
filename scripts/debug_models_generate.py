"""
Debug script for model_generator.py
"""
import sys
import os
import asyncio
import json

# Add project root to Python path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.metadata_store import MetadataService, get_session
from app.services.model_generator import ModelGeneratorService
from app.models.response import ModelConfig
from app.core.logging import logger

async def debug_model_generator(table_name="STTM_CUSTOMER"):
    """Debug model generator directly"""
    logger.info(f"Starting debug session for table: {table_name}")
    
    # Initialize services
    model_service = ModelGeneratorService()
    
    # Log template directory
    logger.info(f"Template directory: {model_service.templates_dir}")
    
    # Check if templates directory exists
    if os.path.exists(model_service.templates_dir):
        logger.info(f"Template directory exists: {os.path.abspath(model_service.templates_dir)}")
        templates = os.listdir(model_service.templates_dir)
        logger.info(f"Templates found: {templates}")
    else:
        logger.error(f"Template directory does not exist: {os.path.abspath(model_service.templates_dir)}")
    
    try:
        # Create DB session
        db = get_session()
        
        # Try to get metadata for the table
        metadata_service = MetadataService()
        metadata_entries = metadata_service.get_metadata_by_table(db, table_name)
        
        if not metadata_entries:
            logger.error(f"No metadata found for table: {table_name}")
            return
        
        logger.info(f"Found {len(metadata_entries)} metadata entries for table {table_name}")
        
        # Create config
        config = ModelConfig(
            table_name=table_name,
            model_type=None,  # Let it auto-detect
            use_ai_enhancement=True
        )
        
        logger.info(f"Using config: {json.dumps(config.dict(), default=str)}")
        
        # Generate model
        try:
            result = await model_service.generate_models(config, db)
            logger.info(f"Model generation successful!")
            logger.info(f"Model type: {result.model_type}")
            logger.info(f"Metadata count: {result.metadata_count}")
            logger.info(f"Warnings: {result.warnings}")
            
            # Save YAML to file
            output_file = f"{table_name}_debug_model.yaml"
            with open(output_file, "w") as f:
                f.write(result.model_yaml)
            logger.info(f"Model YAML saved to: {output_file}")
            
            # Return success
            return True
        except Exception as e:
            logger.error(f"Error generating model: {str(e)}", exc_info=True)
            return False
            
    except Exception as e:
        logger.error(f"Debug error: {str(e)}", exc_info=True)
        return False
    finally:
        if 'db' in locals():
            db.close()

if __name__ == "__main__":
    # Get table name from command line
    table_name = sys.argv[1] if len(sys.argv) > 1 else "STTM_CUSTOMER"
    asyncio.run(debug_model_generator(table_name))
