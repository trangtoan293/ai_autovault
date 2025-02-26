"""
Test script for automatic model type detection and generation
This script tests the enhanced model_generator with automatic model type detection
"""
import sys
import os
import asyncio
from pprint import pprint

# Add project root to Python path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.metadata_store import MetadataService, get_session
from app.services.model_generator import ModelGeneratorService
from app.models.response import ModelConfig

async def test_auto_model_generation(table_name):
    """Test automatic model generation for a table"""
    print(f"\n=== Testing automatic model generation for table: {table_name} ===\n")
    service = ModelGeneratorService()
    db = get_session()
    
    try:
        # Configure with auto model_type (None)
        config = ModelConfig(
            table_name=table_name,
            model_type=None,  # Auto detect model type
            use_ai_enhancement=True  # Required for auto detection
        )
        
        # Generate models
        result = await service.generate_models(config, db)
        
        # Print results
        print(f"Generated {result.metadata_count} metadata entries")
        print(f"Model type: {result.model_type}")
        
        if result.warnings:
            print("\nWarnings:")
            for warning in result.warnings:
                print(f"- {warning}")
        
        print("\nGenerated YAML:")
        print("-" * 80)
        print(result.model_yaml)
        print("-" * 80)
        
        return result
    finally:
        db.close()

async def test_specific_model_generation(table_name, model_type="hub"):
    """Test specific model type generation for a table"""
    print(f"\n=== Testing {model_type} model generation for table: {table_name} ===\n")
    service = ModelGeneratorService()
    db = get_session()
    
    try:
        # Configure with specific model_type
        config = ModelConfig(
            table_name=table_name,
            model_type=model_type,
            use_ai_enhancement=True
        )
        
        # Generate models
        result = await service.generate_models(config, db)
        
        # Print results
        print(f"Generated {result.metadata_count} metadata entries")
        print(f"Model type: {result.model_type}")
        
        if result.warnings:
            print("\nWarnings:")
            for warning in result.warnings:
                print(f"- {warning}")
        
        print("\nGenerated YAML:")
        print("-" * 80)
        print(result.model_yaml)
        print("-" * 80)
        
        return result
    finally:
        db.close()

async def main():
    """Main entry point for test script"""
    import argparse
    parser = argparse.ArgumentParser(description='Test automatic model generation')
    parser.add_argument('--table', required=True, help='Table name to generate models for')
    parser.add_argument('--mode', choices=['auto', 'hub', 'link', 'satellite'], default='auto',
                      help='Mode to test (auto for automatic detection, or specific model type)')
    
    args = parser.parse_args()
    
    if args.mode == 'auto':
        await test_auto_model_generation(args.table)
    else:
        await test_specific_model_generation(args.table, args.mode)

if __name__ == "__main__":
    asyncio.run(main())
