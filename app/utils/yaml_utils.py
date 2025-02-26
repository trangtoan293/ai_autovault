"""
YAML processing utilities
"""
import yaml
from typing import Dict, Any, List, Optional, Union, TextIO
import os
import re

from app.core.logging import logger


def load_yaml(file_path: str) -> Dict[str, Any]:
    """
    Load YAML file
    
    Args:
        file_path: Path to YAML file
        
    Returns:
        Dictionary with YAML content
    """
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    
    except Exception as e:
        logger.error(f"Error loading YAML file {file_path}: {str(e)}")
        raise


def save_yaml(data: Dict[str, Any], file_path: str) -> None:
    """
    Save data as YAML file
    
    Args:
        data: Data to save
        file_path: Output file path
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        
        logger.info(f"Saved YAML file to {file_path}")
    
    except Exception as e:
        logger.error(f"Error saving YAML file {file_path}: {str(e)}")
        raise


def dict_to_yaml(data: Dict[str, Any]) -> str:
    """
    Convert dictionary to YAML string
    
    Args:
        data: Dictionary to convert
        
    Returns:
        YAML string
    """
    try:
        if data is None:
            logger.warning("Attempting to convert None to YAML, returning empty string")
            return ""
        return yaml.dump(data, default_flow_style=False, sort_keys=False)
    
    except Exception as e:
        logger.error(f"Error converting dictionary to YAML: {str(e)}")
        return f"# Error converting to YAML: {str(e)}"


def yaml_to_dict(yaml_str: str) -> Dict[str, Any]:
    """
    Convert YAML string to dictionary
    
    Args:
        yaml_str: YAML string to convert
        
    Returns:
        Dictionary
    """
    try:
        if not yaml_str or not yaml_str.strip():
            logger.warning("Empty YAML string provided")
            return {}
        return yaml.safe_load(yaml_str)
    
    except Exception as e:
        logger.error(f"Error converting YAML to dictionary: {str(e)}")
        raise


def merge_yaml_files(file_paths: List[str]) -> Dict[str, Any]:
    """
    Merge multiple YAML files
    
    Args:
        file_paths: List of YAML file paths
        
    Returns:
        Merged dictionary
    """
    try:
        merged_data = {}
        
        for file_path in file_paths:
            data = load_yaml(file_path)
            merged_data.update(data)
        
        return merged_data
    
    except Exception as e:
        logger.error(f"Error merging YAML files: {str(e)}")
        raise


def validate_yaml_structure(yaml_data: Dict[str, Any], required_keys: List[str]) -> bool:
    """
    Validate YAML structure by checking for required keys
    
    Args:
        yaml_data: YAML data to validate
        required_keys: List of required top-level keys
        
    Returns:
        True if valid, False otherwise
    """
    try:
        if yaml_data is None:
            logger.warning("None provided instead of YAML data")
            return False
            
        for key in required_keys:
            if key not in yaml_data:
                logger.warning(f"Missing required key in YAML: {key}")
                return False
        
        return True
    
    except Exception as e:
        logger.error(f"Error validating YAML structure: {str(e)}")
        return False


def extract_yaml_block(text: str) -> Optional[str]:
    """
    Extract YAML block from text
    
    Args:
        text: Text that may contain a YAML block
        
    Returns:
        YAML block if found, None otherwise
    """
    try:
        if not text:
            return None
            
        # Look for content between --- markers (common YAML block format)
        yaml_pattern = re.compile(r'^---\s*$\n(.*?)^\s*---\s*$', re.MULTILINE | re.DOTALL)
        match = yaml_pattern.search(text)
        
        if match:
            return match.group(1).strip()
            
        # If not found, try to extract any valid YAML at the beginning of the text
        try:
            # Try to parse the text as YAML
            yaml.safe_load(text)
            # If no exception, assume the entire text is YAML
            return text
        except yaml.YAMLError:
            # Not valid YAML
            return None
    
    except Exception as e:
        logger.error(f"Error extracting YAML block: {str(e)}")
        return None


def merge_yaml_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any], overwrite: bool = True) -> Dict[str, Any]:
    """
    Merge two YAML dictionaries with deep merging of nested dictionaries
    
    Args:
        dict1: First dictionary
        dict2: Second dictionary (takes precedence if overwrite=True)
        overwrite: Whether to overwrite values from dict1 with dict2
        
    Returns:
        Merged dictionary
    """
    try:
        if dict1 is None:
            return dict2 or {}
        if dict2 is None:
            return dict1 or {}
            
        result = dict1.copy()
        
        for key, value in dict2.items():
            # If both are dictionaries, merge them recursively
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = merge_yaml_dicts(result[key], value, overwrite)
            # Otherwise just overwrite or skip
            elif key in result and not overwrite:
                continue
            else:
                result[key] = value
                
        return result
    
    except Exception as e:
        logger.error(f"Error merging YAML dictionaries: {str(e)}")
        raise


def deep_update_yaml(file_path: str, update_data: Dict[str, Any]) -> None:
    """
    Update a YAML file with deep merging
    
    Args:
        file_path: Path to YAML file to update
        update_data: Data to update with
    """
    try:
        # Load existing data
        existing_data = load_yaml(file_path) if os.path.exists(file_path) else {}
        
        # Merge data
        updated_data = merge_yaml_dicts(existing_data, update_data)
        
        # Save updated data
        save_yaml(updated_data, file_path)
        
        logger.info(f"Updated YAML file: {file_path}")
    
    except Exception as e:
        logger.error(f"Error updating YAML file {file_path}: {str(e)}")
        raise


def parse_yaml_with_env_vars(yaml_str: str) -> Dict[str, Any]:
    """
    Parse YAML string with environment variable substitution
    
    Args:
        yaml_str: YAML string with ${ENV_VAR} placeholders
        
    Returns:
        Dictionary with environment variables substituted
    """
    try:
        if not yaml_str:
            return {}
            
        # Define pattern for ${ENV_VAR} or $ENV_VAR
        pattern = re.compile(r'\${([^}]+)}|\$([A-Za-z0-9_]+)')
        
        # Define a function to replace environment variables
        def replace_env_vars(match):
            env_var = match.group(1) or match.group(2)
            return os.environ.get(env_var, f"${env_var}")
        
        # Replace environment variables
        yaml_str = pattern.sub(replace_env_vars, yaml_str)
        
        # Parse YAML
        return yaml.safe_load(yaml_str)
    
    except Exception as e:
        logger.error(f"Error parsing YAML with environment variables: {str(e)}")
        raise


def is_valid_yaml(yaml_str: str) -> bool:
    """
    Check if a string is valid YAML
    
    Args:
        yaml_str: String to check
        
    Returns:
        True if valid YAML, False otherwise
    """
    try:
        if not yaml_str:
            return False
        yaml.safe_load(yaml_str)
        return True
    except yaml.YAMLError:
        return False
    except Exception as e:
        logger.error(f"Error validating YAML: {str(e)}")
        return False
