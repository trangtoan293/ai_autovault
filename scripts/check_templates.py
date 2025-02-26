"""
Check template directory and files
"""
import os
import sys

# Add project root to Python path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.config import settings

def check_templates():
    """Check if templates directory and files exist"""
    templates_dir = settings.MODEL_TEMPLATES_DIR
    print(f"Templates directory configured as: {templates_dir}")
    
    # Check if the directory exists
    if os.path.exists(templates_dir):
        print(f"Directory exists: {os.path.abspath(templates_dir)}")
        
        # List template files
        templates = os.listdir(templates_dir)
        print(f"Found {len(templates)} files in templates directory:")
        for i, template in enumerate(templates):
            file_path = os.path.join(templates_dir, template)
            file_size = os.path.getsize(file_path)
            print(f"  {i+1}. {template} ({file_size} bytes)")
    else:
        print(f"ERROR: Directory does not exist: {os.path.abspath(templates_dir)}")
        
        # Try to find the templates directory
        print("\nLooking for templates directories:")
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        for root, dirs, files in os.walk(root_dir):
            if 'templates' in dirs:
                templates_path = os.path.join(root, 'templates')
                print(f"Found templates dir: {templates_path}")
                
                # Check if it has models subdirectory
                models_path = os.path.join(templates_path, 'models')
                if os.path.exists(models_path):
                    print(f"  Found models subdir: {models_path}")
                    print(f"  Files: {os.listdir(models_path)}")

if __name__ == "__main__":
    check_templates()
