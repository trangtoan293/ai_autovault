"""
Test script for new YAML format output
"""
import sys
import os
import asyncio
import yaml
from pprint import pprint

# Add project root to Python path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.metadata_store import MetadataService, get_session
from app.services.model_generator import ModelGeneratorService
from app.models.response import ModelConfig

async def test_yaml_generation(table_name):
    """Test YAML generation with new format for a table"""
    print(f"\n=== Testing YAML generation for table: {table_name} ===\n")
    service = ModelGeneratorService()
    db = get_session()
    
    try:
        # Configure with auto model type detection
        config = ModelConfig(
            table_name=table_name,
            model_type=None,  # Auto detect
            use_ai_enhancement=True
        )
        
        # Generate models
        result = await service.generate_models(config, db)
        
        # Print metadata info
        print(f"Generated model for {result.metadata_count} metadata entries")
        print(f"Model types detected: {result.model_type}")
        
        if result.warnings:
            print("\nWarnings:")
            for warning in result.warnings:
                print(f"- {warning}")
        
        # Parse YAML to validate format
        yamls = result.model_yaml.split("---")
        print(f"\nFound {len(yamls)} YAML documents in result")
        
        # Analyze each YAML document
        for i, yaml_doc in enumerate(yamls):
            if not yaml_doc.strip():
                continue
                
            try:
                yaml_data = yaml.safe_load(yaml_doc)
                
                print(f"\nYAML Document {i+1}:")
                print(f"  Entity Type: {yaml_data.get('target_entity_type', 'unknown')}")
                print(f"  Target Table: {yaml_data.get('target_table', 'unknown')}")
                print(f"  Source Table: {yaml_data.get('source_table', 'unknown')}")
                
                # Check for required fields
                required_fields = ['source_schema', 'source_table', 'target_schema', 
                                  'target_table', 'target_entity_type', 'collision_code']
                missing = [field for field in required_fields if field not in yaml_data]
                
                if missing:
                    print(f"  MISSING REQUIRED FIELDS: {', '.join(missing)}")
                
                # Check column structure
                if 'columns' in yaml_data:
                    print(f"  Columns: {len(yaml_data['columns'])}")
                    
                    for j, col in enumerate(yaml_data['columns'][:3]):  # Show first 3 columns
                        print(f"    {j+1}. {col.get('target')} - {col.get('dtype')} - {col.get('key_type', 'regular')}")
                        
                    if len(yaml_data['columns']) > 3:
                        print(f"    ... and {len(yaml_data['columns']) - 3} more columns")
                else:
                    print("  NO COLUMNS FOUND!")
            except yaml.YAMLError as e:
                print(f"Error parsing YAML document {i+1}: {e}")
                print(yaml_doc)
        
        return result
    finally:
        db.close()

async def main():
    """Main function"""
    import argparse
    parser = argparse.ArgumentParser(description="Test new YAML format generation")
    parser.add_argument("--table", required=True, help="Table name to generate YAML for")
    
    args = parser.parse_args()
    await test_yaml_generation(args.table)

if __name__ == "__main__":
    asyncio.run(main())
