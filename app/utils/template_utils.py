"""
Template rendering utilities
"""
import os
import datetime
from typing import Dict, Any, Optional, List
from jinja2 import Environment, FileSystemLoader, select_autoescape, Template

from app.core.logging import logger


def render_template(template_name: str, template_dir: str, context: Dict[str, Any]) -> str:
    """
    Render a Jinja2 template
    
    Args:
        template_name: Name of the template file
        template_dir: Directory containing templates
        context: Dictionary with template variables
        
    Returns:
        Rendered template as string
    """
    try:
        # Create Jinja2 environment
        env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=select_autoescape(['html', 'xml']),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # Add global functions
        env.globals['now'] = datetime.datetime.now
        
        # Load template
        template = env.get_template(template_name)
        
        # Render template
        rendered = template.render(**context)
        
        return rendered
    
    except Exception as e:
        logger.error(f"Error rendering template {template_name}: {str(e)}")
        raise


def render_string_template(template_string: str, context: Dict[str, Any]) -> str:
    """
    Render a Jinja2 template from a string
    
    Args:
        template_string: Template string
        context: Dictionary with template variables
        
    Returns:
        Rendered template as string
    """
    try:
        # Create template from string
        template = Template(template_string)
        
        # Render template
        rendered = template.render(**context)
        
        return rendered
    
    except Exception as e:
        logger.error(f"Error rendering string template: {str(e)}")
        raise


def list_templates(template_dir: str, pattern: Optional[str] = None) -> List[str]:
    """
    List available templates in directory
    
    Args:
        template_dir: Directory containing templates
        pattern: Optional glob pattern to filter templates
        
    Returns:
        List of template names
    """
    try:
        # Create Jinja2 environment
        env = Environment(loader=FileSystemLoader(template_dir))
        
        # List templates
        templates = env.list_templates(filter_func=lambda x: pattern is None or pattern in x)
        
        return templates
    
    except Exception as e:
        logger.error(f"Error listing templates in {template_dir}: {str(e)}")
        raise


def create_template(template_name: str, template_dir: str, content: str) -> None:
    """
    Create a new template file
    
    Args:
        template_name: Name of the template file
        template_dir: Directory to store the template
        content: Template content
    """
    try:
        # Ensure directory exists
        os.makedirs(template_dir, exist_ok=True)
        
        # Create template file
        template_path = os.path.join(template_dir, template_name)
        
        with open(template_path, 'w') as f:
            f.write(content)
        
        logger.info(f"Created template: {template_path}")
    
    except Exception as e:
        logger.error(f"Error creating template {template_name}: {str(e)}")
        raise


def render_template_to_file(template_name: str, template_dir: str, output_path: str, context: Dict[str, Any]) -> None:
    """
    Render a template and save to file
    
    Args:
        template_name: Name of the template file
        template_dir: Directory containing templates
        output_path: Path to save rendered template
        context: Dictionary with template variables
    """
    try:
        # Render template
        rendered = render_template(template_name, template_dir, context)
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to file
        with open(output_path, 'w') as f:
            f.write(rendered)
        
        logger.info(f"Rendered template to {output_path}")
    
    except Exception as e:
        logger.error(f"Error rendering template to file: {str(e)}")
        raise


def render_multiple_templates(template_dir: str, output_dir: str, context: Dict[str, Any], patterns: Optional[List[str]] = None) -> List[str]:
    """
    Render multiple templates matching patterns
    
    Args:
        template_dir: Directory containing templates
        output_dir: Directory to save rendered templates
        context: Dictionary with template variables
        patterns: Optional list of glob patterns to filter templates
        
    Returns:
        List of paths to rendered files
    """
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Get templates to render
        templates_to_render = []
        if patterns:
            for pattern in patterns:
                templates_to_render.extend(list_templates(template_dir, pattern))
        else:
            templates_to_render = list_templates(template_dir)
        
        # Render each template
        rendered_files = []
        for template_name in templates_to_render:
            # Determine output path
            output_path = os.path.join(output_dir, template_name)
            if output_path.endswith('.j2'):
                output_path = output_path[:-3]  # Remove .j2 extension
            
            # Create output directory if needed
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Render and save
            render_template_to_file(template_name, template_dir, output_path, context)
            rendered_files.append(output_path)
        
        return rendered_files
    
    except Exception as e:
        logger.error(f"Error rendering multiple templates: {str(e)}")
        raise
