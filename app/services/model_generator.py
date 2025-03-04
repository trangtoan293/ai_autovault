"""
AI-based Data Vault 2.0 model generation
Enhanced with two-phase analysis and specialized validation
"""
import os
import json
import re
from typing import List, Dict, Any, Optional, Union, Set
from fastapi import HTTPException
from sqlalchemy.orm import Session
from tenacity import retry, stop_after_attempt, wait_exponential
from jinja2 import Environment, FileSystemLoader
import pandas as pd
from datetime import datetime

# LangChain imports
from langchain_core.prompts import ChatPromptTemplate

from app.core.config import settings
from app.core.logging import logger
from app.models.response import ModelGenerationResponse, ModelConfig
from app.utils.template_utils import render_template
from app.utils.yaml_utils import dict_to_yaml
from app.services.metadata_store import MetadataService



class DataVaultModelState:
    """State management for Data Vault model generation process"""
    
    def __init__(self):
        self.metadata_content = None
        self.metadata_df = None
        self.hub_link_analysis = None
        self.satellite_analysis = None
        self.final_analysis = None
        self.warnings = []
        self.llm_analysis_result = None
        # Store validated metadata for cross-reference
        self.hubs_metadata = {}
        self.links_metadata = {}
        self.sats_metadata = {}
        self.lsats_metadata = {}

class DataVaultValidationError(Exception):
    """Exception raised for Data Vault validation errors"""
    pass


class ModelGeneratorService:
    """Service for AI-based Data Vault model generation"""
    model_yaml=None
    def __init__(self):
        self.metadata_service = MetadataService()
        self.templates_dir = settings.MODEL_TEMPLATES_DIR
        
        # Initialize LLM model using LangChain
        self.llm = None
        self._init_llm()
            
        # State management
        self.state = DataVaultModelState()
    
    def _init_llm(self):
        """Initialize the LLM model"""
        try:
            # Import here to avoid loading unless needed
            from langchain.chat_models import init_chat_model
            
            # Check for API key in environment
            groq_api_key = os.environ.get("GROQ_API_KEY") or settings.GROQ_API_KEY
            if not groq_api_key:
                logger.warning("GROQ_API_KEY not found in environment variables or settings")
                return
            
            # Initialize chat model
            model_name = settings.LLM_MODEL
            provider = settings.LLM_PROVIDER
            
            print(model_name)
            print(provider)
            self.llm = init_chat_model(model_name, model_provider=provider)
            logger.info(f"LLM model initialized with {provider} provider using model {model_name}")
        except Exception as e:
            logger.error(f"Error initializing LLM model: {str(e)}")
            # Continue without LLM
    
    async def generate_models_from_source(self, source_system: str, table_name: str, db: Session) -> ModelGenerationResponse:
        """
        Generate Data Vault models based on source system and table name.
        This is the main method that implements the new requirements.
        """
        try:
            # Get metadata for the specified source system and table
            metadata_entries = self.metadata_service.get_metadata_by_source_and_table(db, source_system, table_name)
            
            if not metadata_entries:
                raise HTTPException(status_code=404, 
                                    detail=f"No metadata found for source system {source_system} and table {table_name}")
            
            # Create formatted metadata content for LLM processing
            metadata_content = self._format_metadata_for_llm(metadata_entries)
            
            # Store in state
            self.state.metadata_content = metadata_content
            
            # Call LLM to analyze metadata and generate Data Vault components
            llm_result = await self._analyze_metadata_with_llm(metadata_content)
            self.state.llm_analysis_result = llm_result
            
            # Store LLM analysis result in the final_analysis state for validation
            self.state.final_analysis = llm_result
            
            # Generate YAML models from LLM analysis
            generated_models = []
            
            # Process components from LLM result
            if llm_result:
                logger.debug(f"Processing LLM analysis results for table {table_name} from source {source_system}")
                
                # Process hubs
                hubs = llm_result.get("hubs", [])
                if hubs and isinstance(hubs, list):
                    logger.debug(f"Processing {len(hubs)} hubs from LLM result")
                    for hub in hubs:
                        if not isinstance(hub, dict):
                            logger.warning(f"Skipping non-dict hub: {hub}")
                            continue
                            
                        try:
                            logger.debug(f"Processing hub: {hub.get('name', 'unknown')}")
                            model_yaml = None
                            model_yaml = self._generate_hub_yaml(hub, source_system, table_name, metadata_entries)
                            if model_yaml:
                                generated_models.append(model_yaml)
                                logger.debug(f"Generated YAML for hub: {hub.get('name', 'unknown')}")
                        except Exception as e:
                            logger.error(f"Error processing hub {hub.get('name', 'unknown')}: {str(e)}", exc_info=True)
                            self.state.warnings.append(f"Error processing hub {hub.get('name', 'unknown')}: {str(e)}")
                
                # Process links
                links = llm_result.get("links", [])
                if links and isinstance(links, list):
                    logger.debug(f"Processing {len(links)} links from LLM result")
                    for link in links:
                        if not isinstance(link, dict):
                            logger.warning(f"Skipping non-dict link: {link}")
                            continue
                            
                        try:
                            logger.debug(f"Processing link: {link.get('name', 'unknown')}")
                            model_yaml = None
                            model_yaml = self._generate_link_yaml(link, source_system, table_name, metadata_entries)
                            if model_yaml:
                                generated_models.append(model_yaml)
                                logger.debug(f"Generated YAML for link: {link.get('name', 'unknown')}")
                        except Exception as e:
                            logger.error(f"Error processing link {link.get('name', 'unknown')}: {str(e)}", exc_info=True)
                            self.state.warnings.append(f"Error processing link {link.get('name', 'unknown')}: {str(e)}")
                
                # Process satellites
                satellites = llm_result.get("satellites", [])
                if satellites and isinstance(satellites, list):
                    logger.debug(f"Processing {len(satellites)} satellites from LLM result")
                    for sat in satellites:
                        if not isinstance(sat, dict):
                            logger.warning(f"Skipping non-dict satellite: {sat}")
                            continue
                            
                        try:
                            logger.debug(f"Processing satellite: {sat.get('name', 'unknown')}")
                            model_yaml = None
                            model_yaml = self._generate_satellite_yaml(sat, source_system, table_name, metadata_entries)
                            if model_yaml:
                                generated_models.append(model_yaml)
                                logger.debug(f"Generated YAML for satellite: {sat.get('name', 'unknown')}")
                        except Exception as e:
                            logger.error(f"Error processing satellite {sat.get('name', 'unknown')}: {str(e)}", exc_info=True)
                            self.state.warnings.append(f"Error processing satellite {sat.get('name', 'unknown')}: {str(e)}")
                
                # Process link satellites
                link_satellites = llm_result.get("link_satellites", [])
                if link_satellites and isinstance(link_satellites, list):
                    logger.debug(f"Processing {len(link_satellites)} link satellites from LLM result")
                    for lsat in link_satellites:
                        if not isinstance(lsat, dict):
                            logger.warning(f"Skipping non-dict link satellite: {lsat}")
                            continue
                            
                        try:
                            logger.debug(f"Processing link satellite: {lsat.get('name', 'unknown')}")
                            model_yaml = None
                            model_yaml = self._generate_link_satellite_yaml(lsat, source_system, table_name, metadata_entries)
                            if model_yaml:
                                generated_models.append(model_yaml)
                                logger.debug(f"Generated YAML for link satellite: {lsat.get('name', 'unknown')}")
                        except Exception as e:
                            logger.error(f"Error processing link satellite {lsat.get('name', 'unknown')}: {str(e)}", exc_info=True)
                            self.state.warnings.append(f"Error processing link satellite {lsat.get('name', 'unknown')}: {str(e)}")
                
            
            # If no models were generated, return a message
            if not generated_models:
                logger.warning(f"No models could be generated for {table_name}")
                return ModelGenerationResponse(
                    message="ERROR",
                    model_yaml=f"# No models could be generated for {table_name}",
                    table_name=table_name,
                    model_type="auto",
                    metadata_count=len(metadata_entries),
                    warnings=self.state.warnings
                )
            
            # Combine all generated models
            combined_yaml = "\n---\n".join(generated_models)
            
            logger.info(f"Generated {len(generated_models)} models for {table_name} from {source_system}")
            
            model_result = ModelGenerationResponse(
                message="DONE",
                model_yaml= combined_yaml,
                table_name=table_name,
                model_type="auto",
                metadata_count=len(metadata_entries),
                warnings=self.state.warnings
            )
            
            return model_result

        except Exception as e:
            logger.error(f"Error generating model from source: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error generating model from source: {str(e)}")

    async def generate_models(self, config: ModelConfig, db: Session) -> ModelGenerationResponse:
        """
        Generate Data Vault models based on metadata and configuration.
        If model_type is not specified, all appropriate model types will be generated based on AI analysis.
        """
        try:
            # For compatibility with existing implementation
            # Get metadata for the specified table
            metadata_entries = self.metadata_service.get_metadata_by_table(db, config.table_name)
            
            if not metadata_entries:
                raise HTTPException(status_code=404, detail=f"No metadata found for table {config.table_name}")
            
            # Get source_system from the first metadata entry
            source_system = metadata_entries[0].source_system
            
            # Use the new implementation
            model_result = await self.generate_models_from_source(source_system, config.table_name, db)
            return model_result
            
        except DataVaultValidationError as e:
            logger.error(f"Data Vault validation error: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Data Vault validation error: {str(e)}")
        except Exception as e:
            logger.error(f"Error generating model: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error generating model: {str(e)}")

    async def preview_model(self, metadata_id: int, model_type: str, db: Session) -> ModelGenerationResponse:
        """
        Preview a data model based on specific metadata
        """
        # Get specific metadata entry
        metadata_entry = self.metadata_service.get_metadata(db, metadata_id)
        
        if not metadata_entry:
            raise HTTPException(status_code=404, detail=f"Metadata with ID {metadata_id} not found")
        
        # Create minimal config for preview
        config = ModelConfig(
            table_name=metadata_entry.table_name,
            model_type=model_type,
            use_ai_enhancement=True
        )
        
        # Generate preview model
        return await self.generate_models(config, db)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _call_llm_with_prompt(self, prompt_template: str, input_variables: Dict[str, Any]) -> Dict[str, Any]:
        """
        Call LLM with a prompt template and parse JSON response
        """
        try:
            if not self.llm:
                raise ValueError("LLM model not initialized")
                
            # Create a prompt template and chain
            prompt = ChatPromptTemplate.from_template(prompt_template)
            chain = prompt | self.llm
            
            # Log the input variables to help with debugging
            logger.debug(f"Calling LLM with variables: {list(input_variables.keys())}")
            for key, value in input_variables.items():
                if isinstance(value, str) and len(value) > 100:
                    logger.debug(f"Variable {key}: {value[:100]}... (truncated, total length: {len(value)})")
                else:
                    logger.debug(f"Variable {key}: {value}")
            
            # Call the LLM and get the response
            result = await chain.ainvoke(input_variables)
            
            # Extract JSON from the response
            content = result.content
            logger.debug(f"LLM response (first 500 chars): {content[:500]}...")
            
            # Extract JSON from response
            json_match = re.search(r"```(?:json)?\n(.*?)\n```", content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to extract JSON without code blocks
                json_match = re.search(r"({.*})", content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    logger.error(f"Could not extract JSON from LLM response. Response content: {content}")
                    # Return a default structure as fallback
                    return {
                        "hubs": [],
                        "links": [],
                        "satellites": [],
                        "link_satellites": []
                    }
            
            # Parse JSON
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON: {str(e)}. JSON string: {json_str}")
                # Return a default structure as fallback
                return {
                    "hubs": [],
                    "links": [],
                    "satellites": [],
                    "link_satellites": []
                }
        except Exception as e:
            logger.error(f"Error calling LLM: {str(e)}")
            # Return a default structure as fallback
            return {
                "hubs": [],
                "links": [],
                "satellites": [],
                "link_satellites": []
            }

    def _map_column_metadata(self, column_name: str, metadata_entries: List) -> Dict[str, Any]:
        """
        Map column name to its metadata from source
        Returns a dictionary with data type and other properties
        """
        for entry in metadata_entries:
            if entry.column_name == column_name:
                return {
                    "name": entry.column_name,
                    "dtype": entry.data_type,
                    "description": entry.description,
                    "is_primary_key": entry.is_primary_key,
                    "is_foreign_key": entry.is_foreign_key,
                    "nullable": entry.nullable,
                    "source_system": entry.source_system,
                    "table_name": entry.table_name
                }
        
        # Default if not found
        return {
            "name": column_name,
            "dtype": "VARCHAR2(255)",
            "description": f"Auto-generated for {column_name}",
            "is_primary_key": False,
            "is_foreign_key": False,
            "nullable": True
        }
    
    def _format_metadata_for_llm(self, metadata_entries) -> str:
        """
        Format metadata entries into a concise, structured format for LLM prompts
        This helps reduce context size and improve LLM understanding
        """
        if not metadata_entries:
            return "No metadata available"
        
        # Group by table
        tables = {}
        for entry in metadata_entries:
            if entry.table_name not in tables:
                tables[entry.table_name] = {
                    'description': entry.additional_properties.get('table_description', '') if entry.additional_properties else '',
                    'schema': entry.additional_properties.get('schema_name', entry.source_system) if entry.additional_properties else entry.source_system,
                    'columns': []
                }
            
            # Add column info in a concise format
            column_info = {
                'name': entry.column_name,
                'type': entry.data_type,
                'description': entry.description,
                'attributes': []
            }
            
            # Add attributes
            if entry.is_primary_key:
                column_info['attributes'].append('PK')
            
            if entry.is_foreign_key:
                column_info['attributes'].append(f"FK -> {entry.foreign_key_table}.{entry.foreign_key_column}")
            
            if not entry.nullable:
                column_info['attributes'].append('NOT NULL')
                
            tables[entry.table_name]['columns'].append(column_info)
        
        # Format into a readable string
        formatted_parts = []
        for table_name, table_info in tables.items():
            # Table header
            formatted_parts.append(f"TABLE: {table_name} ({table_info['schema']})")
            
            if table_info['description']:
                formatted_parts.append(f"DESCRIPTION: {table_info['description']}")
                
            formatted_parts.append("COLUMNS:")
            
            # Format columns
            for col in table_info['columns']:
                col_str = f"- {col['name']} ({col['type']})"
                
                if col['attributes']:
                    col_str += f" {', '.join(col['attributes'])}"
                    
                if 'description' in col and col['description']:
                    col_str += f" - {col['description']}"
                    
                formatted_parts.append(col_str)
                
            formatted_parts.append("")  # Empty line between tables
        
        return "\n".join(formatted_parts)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _analyze_metadata_with_llm(self, metadata_content: str) -> Dict[str, Any]:
        """
        Call LLM with metadata content to analyze and generate Data Vault components
        """
        try:
            if not self.llm:
                raise ValueError("LLM model not initialized")
                
            # Create prompt template
            prompt_template = """
You are a Data Vault 2.0 modeling expert in banking domain.
Analyze the table metadata given by user and recommend appropriate HUB, LINK, SATELLITE, and LINK SATELLITE components.

TABLE METADATA:
{metadata}

Based on this metadata, design a complete Data Vault 2.0 model following these rules:
1. A Hub component represents a unique business object
2. A Link component represents a relationship between Hubs
3. A Satellite component contains descriptive attributes for a Hub
4. A Link Satellite component contains descriptive attributes for a Link
5. A component can be derived from multiple source tables
6. A Link component must include at least 2 Hub components in relationships

Think step by step and provide the result in this specific JSON format:
{{    "hubs": [
        {{
            "name": Hub component name, it should be in the format of HUB_<business_object_name>,
            "business_keys": List of business key columns,
            "source_tables": List of source tables,
            "description": Short description of the component
        }}
    ],
    "links": [
        {{
            "name": Link component name, it should be in the format of LNK_<relationship_name>,
            "related_hubs": List of related hubs,
            "business_keys": List of business key columns, including all bussiness keys from related hubs,
            "source_tables": List of source tables,
            "description": Short description of the component
        }}
    ],
    "satellites": [
        {{
            "name": Satellite component name, it should be in the format of SAT_<business_object_name>_<description>,
            "hub": Related Hub component name,
            "business_keys": List of business key columns from related Hub component,
            "source_table": Source table name,
            "descriptive_attrs": List of descriptive attributes,must have at least 1 attribute. If there is no attribute, please remove this satellite component.
        }}
    ],
    "link_satellites": [
        {{
            "name": Satellite component name, it should be in the format of LSAT_<relationship_name>_<description>,
            "link": Related Link component name,
            "business_keys": List of business key columns from related Link component,
            "source_table": Source table name - only 1 source table,
            "descriptive_attrs": List of descriptive attributes,must have at least 1 attribute. If there is no attribute, please remove this link satellite component.
        }}
    ]
}}
"""
            
            # Create chain
            prompt = ChatPromptTemplate.from_template(prompt_template)
            chain = prompt | self.llm
            
            # Call the LLM and get the response
            result = await chain.ainvoke({"metadata": metadata_content})
            
            # Extract JSON from the response
            content = result.content
            logger.debug(f"LLM response (first 500 chars): {content[:500]}...")
            
            # Extract JSON from response
            json_match = re.search(r"```(?:json)?\n(.*?)\n```", content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to extract JSON without code blocks
                json_match = re.search(r"({.*})", content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    logger.error(f"Could not extract JSON from LLM response. Response content: {content}")
                    return {
                        "hubs": [],
                        "links": [],
                        "satellites": [],
                        "link_satellites": []
                    }
            
            # Parse JSON
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON: {str(e)}. JSON string: {json_str}")
                return {
                    "hubs": [],
                    "links": [],
                    "satellites": [],
                    "link_satellites": []
                }
        except Exception as e:
            logger.error(f"Error calling LLM: {str(e)}")
            return {
                "hubs": [],
                "links": [],
                "satellites": [],
                "link_satellites": []
            }
    
    def _generate_hub_yaml(self, hub: Dict[str, Any], source_system: str, table_name: str, metadata_entries: List) -> str:
        """
        Generate YAML for a Hub component from LLM analysis
        """
        try:
            # Create a dictionary structure for the Hub YAML
            hub_yaml = {
                "source_schema": source_system,
                "source_table": table_name,
                "target_schema": hub.get("target_schema", settings.DEFAULT_TARGET_SCHEMA),
                "target_table": hub["name"],
                "target_entity_type": "hub",
                "collision_code": hub.get("collision_code", settings.DEFAULT_COLLISION_CODE),
                "description": hub["description"],
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "version": "1.0.0"
                },
                "columns": []
            }
            
            # Add hash key column
            hub_yaml["columns"].append({
                "target": f"DV_HKEY_{hub['name']}",
                "description": f"Hash key for {hub['name']} driven by {hub["business_keys"]}",
                "dtype": "raw",
                "key_type": "hash_key_hub",
                "source": hub["business_keys"]
            })
            
            # Add business key columns using mapping
            for biz_key in hub["business_keys"]:
                # Get full metadata for this column
                col_metadata = self._map_column_metadata(biz_key, metadata_entries)
                
                hub_yaml["columns"].append({
                    "target": biz_key,
                    "dtype": col_metadata["dtype"],
                    "key_type": "biz_key",
                    "source": {
                        "name": col_metadata["name"],
                        "dtype": col_metadata["dtype"],
                        "description": col_metadata["description"]
                    }
                })
            
            # Convert to YAML string
            return dict_to_yaml(hub_yaml)
        except Exception as e:
            logger.error(f"Error generating Hub YAML: {str(e)}")
            self.state.warnings.append(f"Error generating Hub YAML: {str(e)}")
            return None
    
    def _generate_link_yaml(self, link: Dict[str, Any], source_system: str, table_name: str, metadata_entries: List) -> str:
        """
        Generate YAML for a Link component from LLM analysis
        """
        try:
            # Create a dictionary structure for the Link YAML
            link_yaml = {
                "source_schema": source_system,
                "source_table": table_name,
                "target_schema": link.get("target_schema", settings.DEFAULT_TARGET_SCHEMA),
                "target_table": link["name"],
                "target_entity_type": "lnk",
                "collision_code": link.get("collision_code", settings.DEFAULT_COLLISION_CODE),
                "description": link["description"],
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "version": "1.0.0"
                },
                "columns": []
            }
            
            # Add hash key column for the link
            link_yaml["columns"].append({
                "target": f"DV_HKEY_{link['name']}",
                "description": f"Hash key for {link['name']} driven by {link["business_keys"]}",
                "dtype": "raw",
                "key_type": "hash_key_lnk",
                "source": link["business_keys"]
            })
            
            # Add hash key columns for related hubs
            for hub in link["related_hubs"]:
                link_yaml["columns"].append({
                    "target": f"DV_HKEY_{hub}",
                    "dtype": "raw",
                    "key_type": "hash_key_hub",
                    "parent": hub
                })
            
            # Convert to YAML string
            return dict_to_yaml(link_yaml)
        except Exception as e:
            logger.error(f"Error generating Link YAML: {str(e)}")
            self.state.warnings.append(f"Error generating Link YAML: {str(e)}")
            return None
    
    def _generate_satellite_yaml(self, sat: Dict[str, Any], source_system: str, table_name: str, metadata_entries: List) -> str:
        """
        Generate YAML for a Satellite component from LLM analysis
        """
        try:
            # Skip if no descriptive attributes
            if not sat.get("descriptive_attrs") or len(sat["descriptive_attrs"]) == 0:
                logger.warning(f"Skipping satellite {sat['name']} with no descriptive attributes")
                return None
                
            # Create a dictionary structure for the Satellite YAML
            sat_yaml = {
                "source_schema": source_system,
                "source_table": sat["source_table"],
                "target_schema": sat.get("target_schema", settings.DEFAULT_TARGET_SCHEMA),
                "target_table": sat["name"],
                "target_entity_type": "sat",
                "collision_code": sat.get("collision_code", settings.DEFAULT_COLLISION_CODE),
                "description": sat.get("description", f"Satellite for {sat['hub']}"),
                "parent_table": sat["hub"],
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "version": "1.0.0"
                },
                "columns": []
            }
            
            # Add hash key columns
            sat_yaml["columns"].append({
                "target": f"DV_HKEY_{sat['name']}",
                "description": f"Hash key for {sat['name']} driven by {sat["business_keys"]} and data_vault_system columns",
                "dtype": "raw",
                "key_type": "hash_key_sat"
            })
            
            sat_yaml["columns"].append({
                "target": f"DV_HKEY_{sat['hub']}",
                "dtype": "raw",
                "key_type": "hash_key_hub",
                "source": sat["business_keys"]
            })
            
            sat_yaml["columns"].append({
                "target": "DV_HSH_DIFF",
                "description": f"Combination of hash values from attributes in {sat['descriptive_attrs']}",
                "dtype": "raw",
                "key_type": "hash_diff"
            })
            
            # Add descriptive attribute columns using mapping
            for attr in sat["descriptive_attrs"]:
                # Get full metadata for this column
                col_metadata = self._map_column_metadata(attr, metadata_entries)
                
                sat_yaml["columns"].append({
                    "target": attr,
                    "dtype": col_metadata["dtype"],
                    "description": col_metadata["description"],
                    "source": {
                        "name": col_metadata["name"],
                        "dtype": col_metadata["dtype"],
                        "description": col_metadata["description"]
                    }
                })
            
            # Convert to YAML string
            return dict_to_yaml(sat_yaml)
        except Exception as e:
            logger.error(f"Error generating Satellite YAML: {str(e)}")
            self.state.warnings.append(f"Error generating Satellite YAML: {str(e)}")
            return None
    
    def _generate_link_satellite_yaml(self, lsat: Dict[str, Any], source_system: str, table_name: str, metadata_entries: List) -> str:
        """
        Generate YAML for a Link Satellite component from LLM analysis
        """
        try:
            # Skip if no descriptive attributes
            if not lsat.get("descriptive_attrs") or len(lsat["descriptive_attrs"]) == 0:
                logger.warning(f"Skipping link satellite {lsat['name']} with no descriptive attributes")
                return None
                
            # Create a dictionary structure for the Link Satellite YAML
            lsat_yaml = {
                "source_schema": source_system,
                "source_table": lsat["source_table"],
                "target_schema": lsat.get("target_schema", settings.DEFAULT_TARGET_SCHEMA),
                "target_table": lsat["name"],
                "target_entity_type": "lsat",
                "collision_code": lsat.get("collision_code", settings.DEFAULT_COLLISION_CODE),
                "description": lsat.get("description", f"Link Satellite for {lsat['link']}"),
                "parent_table": lsat["link"],
                "metadata": {
                    "created_at": datetime.now().isoformat(),
                    "version": "1.0.0"
                },
                "columns": []
            }
            
            # Add hash key columns
            lsat_yaml["columns"].append({
                "target": f"DV_HKEY_{lsat['name']}",
                "description": f"Hash key for {lsat['name']} driven by {lsat["business_keys"]} and data_vault_system columns",
                "dtype": "raw",
                "key_type": "hash_key_sat"
            })
            
            lsat_yaml["columns"].append({
                "target": f"DV_HKEY_{lsat['link']}",
                "dtype": "raw",
                "key_type": "hash_key_lnk",
                "source": lsat["business_keys"]
            })
            
            lsat_yaml["columns"].append({
                "target": "DV_HSH_DIFF",
                "description": f"Combination of hash values from attributes in {lsat['descriptive_attrs']}",
                "dtype": "raw",
                "key_type": "hash_diff"
            })
            
            # Add descriptive attribute columns using mapping
            for attr in lsat["descriptive_attrs"]:
                # Get full metadata for this column
                col_metadata = self._map_column_metadata(attr, metadata_entries)
                
                lsat_yaml["columns"].append({
                    "target": attr,
                    "dtype": col_metadata["dtype"],
                    "description": col_metadata["description"],
                    "source": {
                        "name": col_metadata["name"],
                        "dtype": col_metadata["dtype"],
                        "description": col_metadata["description"]
                    }
                })
            
            # Convert to YAML string
            return dict_to_yaml(lsat_yaml)
        except Exception as e:
            logger.error(f"Error generating Link Satellite YAML: {str(e)}")
            self.state.warnings.append(f"Error generating Link Satellite YAML: {str(e)}")
            return None
    
    def get_available_templates(self) -> List[str]:
        """
        Get list of available model templates
        """
        try:
            return [f.split('.')[0] for f in os.listdir(self.templates_dir) if f.endswith('.yml.j2')]
        except Exception as e:
            logger.error(f"Error listing templates: {str(e)}")
            raise