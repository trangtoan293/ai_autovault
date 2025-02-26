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

# LangChain imports
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

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
            
            self.llm = init_chat_model(model_name, model_provider=provider)
            logger.info(f"LLM model initialized with {provider} provider using model {model_name}")
        except Exception as e:
            logger.error(f"Error initializing LLM model: {str(e)}")
            # Continue without LLM
    
    async def generate_models(self, config: ModelConfig, db: Session) -> ModelGenerationResponse:
        """
        Generate Data Vault models based on metadata and configuration.
        If model_type is not specified, all appropriate model types will be generated based on AI analysis.
        """
        try:
            # Get metadata for the specified table
            metadata_entries = self.metadata_service.get_metadata_by_table(db, config.table_name)
            
            if not metadata_entries:
                raise HTTPException(status_code=404, detail=f"No metadata found for table {config.table_name}")
            
            # Convert metadata to DataFrame for easier processing
            df = self._convert_metadata_to_df(metadata_entries)
            
            # Create optimized metadata content for LLM processing
            metadata_content = self._format_metadata_for_llm(metadata_entries)
            
            # Store in state
            self.state.metadata_content = metadata_content
            self.state.metadata_df = df
            
            # Create context for template rendering
            context = {
                "table_name": config.table_name,
                "model_type": config.model_type,
                "columns": [self._metadata_to_dict(entry) for entry in metadata_entries],
                "config": config.model_dump(),
                "metadata_content": metadata_content
            }
            
            # Use AI to enhance model if enabled
            if config.use_ai_enhancement and self.llm:
                # Perform two-phase analysis passing only the formatted metadata content
                analysis_result = await self._perform_two_phase_analysis(metadata_content)
                
                # Update context with AI results if analysis was successful
                if analysis_result:
                    context["ai_suggestions"] = analysis_result
            
            # For debugging
            logger.debug(f"Templates directory: {self.templates_dir}")
            logger.debug(f"AI suggestions available: {bool(context.get('ai_suggestions'))}")
            
            # Determine model types to generate
            model_types = []
            
            # If model_type is specified in config, use it
            if config.model_type and config.model_type != "auto":
                model_types = [config.model_type]
            # Otherwise, determine model types from analysis
            elif config.use_ai_enhancement and self.state.final_analysis:
                # Automatically determine model types based on analysis results
                if self.state.final_analysis.get("hubs"):
                    model_types.append("hub")
                if self.state.final_analysis.get("links"):
                    model_types.append("link")
                if self.state.final_analysis.get("satellites") or self.state.final_analysis.get("link_satellites"):
                    model_types.append("satellite")
            # Default to all types if no AI or specified type
            if not model_types:
                model_types = ["hub", "link", "satellite"]
                
            # Render models from templates
            models_yaml = []
            for model_type in model_types:
                # Update context with current model type
                context["model_type"] = model_type
                template_file = f"{model_type}.yml.j2"
                try:
                    # Check if template exists
                    import os
                    template_path = os.path.join(self.templates_dir, template_file)
                    if not os.path.exists(template_path):
                        logger.error(f"Template file not found: {template_path}")
                        self.state.warnings.append(f"Template file not found: {template_path}")
                        continue
                        
                    model_yaml = render_template(template_file, self.templates_dir, context)
                    models_yaml.append(model_yaml)
                except Exception as e:
                    logger.warning(f"Error rendering template for {model_type}: {str(e)}")
                    self.state.warnings.append(f"Failed to generate {model_type} model: {str(e)}")
            
            # Combine all generated models
            if models_yaml:
                model_yaml = "\n---\n".join(models_yaml)
            else:
                # Fallback if no models were generated
                logger.warning("No models were generated. Using fallback template.")
                model_yaml = f"# No models could be generated for {config.table_name}\n# Please check the logs for errors"
            
            # Validate the Data Vault model if we used AI
            if config.use_ai_enhancement and self.state.final_analysis:
                self.validate_data_vault_model(self.state.final_analysis)
            
            # Return the generated model
            return ModelGenerationResponse(
                model_yaml=model_yaml,
                table_name=config.table_name,
                model_type=config.model_type or "auto",
                metadata_count=len(metadata_entries),
                warnings=self.state.warnings
            )
            
        except DataVaultValidationError as e:
            logger.error(f"Data Vault validation error: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Data Vault validation error: {str(e)}")
        except Exception as e:
            logger.error(f"Error generating model: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error generating model: {str(e)}")
    
    def get_available_templates(self) -> List[str]:
        """
        Get list of available model templates
        """
        try:
            return [f.split('.')[0] for f in os.listdir(self.templates_dir) if f.endswith('.yml.j2')]
        except Exception as e:
            logger.error(f"Error listing templates: {str(e)}")
            raise
    
    async def generate_hierarchical_models(self, source_system: str, db: Session) -> ModelGenerationResponse:
        """
        Generate Data Vault models based on hierarchical metadata structure
        """
        try:
            # Get all metadata for source system
            metadata_entries = self.metadata_service.get_metadata_by_source_system(db, source_system)
            
            if not metadata_entries:
                raise HTTPException(status_code=404, detail=f"No metadata found for source system {source_system}")
            
            # Convert to hierarchical structure
            from app.models.metadata import convert_to_hierarchical
            hierarchical_metadata = convert_to_hierarchical(metadata_entries)
            
            # Create optimized metadata content for LLM processing
            metadata_content = self._format_metadata_for_llm(metadata_entries)
            
            # Store in state
            self.state.metadata_content = metadata_content
            self.state.metadata_df = self._convert_metadata_to_df(metadata_entries)
            
            # Create context for template rendering
            context = {
                "source_system": source_system,
                "hierarchical_metadata": hierarchical_metadata,
                "source_systems": hierarchical_metadata.source_systems,
                "use_hierarchical": True,
                "metadata_content": metadata_content,
                "tables": {}
            }
            
            # Group tables by source system
            for system in hierarchical_metadata.source_systems:
                for table in system.tables:
                    if table.name not in context["tables"]:
                        context["tables"][table.name] = {
                            "schema": table.schema,
                            "columns": table.columns,
                            "description": table.description
                        }
            
            # Check if we should use AI
            if self.llm:
                # Perform analysis - pass only the formatted metadata content
                analysis_result = await self._perform_two_phase_analysis(metadata_content)
                
                # Update context with AI results if analysis was successful
                if analysis_result:
                    context["ai_suggestions"] = analysis_result
                
                # Validate the Data Vault model if we used AI
                if self.state.final_analysis:
                    self.validate_data_vault_model(self.state.final_analysis)
            
            # Render hub, link, and sat models
            model_types = ["hub", "link", "satellite"]
            models_yaml = []
            
            for model_type in model_types:
                template_file = f"{model_type}.yml.j2"
                context["model_type"] = model_type
                model_yaml = render_template(template_file, self.templates_dir, context)
                models_yaml.append(model_yaml)
            
            # Combine all generated models
            combined_yaml = "\n---\n".join(models_yaml)
            
            # Return the generated models
            return ModelGenerationResponse(
                model_yaml=combined_yaml,
                table_name="multiple_tables",
                model_type="multiple",
                metadata_count=len(metadata_entries),
                warnings=self.state.warnings
            )
                
        except DataVaultValidationError as e:
            logger.error(f"Data Vault validation error: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Data Vault validation error: {str(e)}")
        except Exception as e:
            logger.error(f"Error generating hierarchical models: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error generating hierarchical models: {str(e)}")
        
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
            use_ai_enhancement=False
        )
        
        # Generate preview model
        return await self.generate_models(config, db)
    
    async def batch_generate_models(self, metadata_ids: List[int], model_type: str, db: Session) -> ModelGenerationResponse:
        """
        Generate multiple models in batch using parallel processing
        """
        import asyncio
        
        tasks = []
        
        for metadata_id in metadata_ids:
            tasks.append(self.preview_model(metadata_id, model_type, db))
        
        # Run tasks in parallel
        models_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        valid_models = []
        tables_processed = set()
        warnings = []
        
        for result in models_results:
            if isinstance(result, Exception):
                logger.error(f"Error in batch processing: {str(result)}")
                warnings.append(f"Error processing model: {str(result)}")
                continue
                
            valid_models.append(result.model_yaml)
            tables_processed.add(result.table_name)
            if hasattr(result, 'warnings') and result.warnings:
                warnings.extend(result.warnings)
        
        # Combine all generated models
        combined_yaml = "\n---\n".join(valid_models)
        
        return ModelGenerationResponse(
            model_yaml=combined_yaml,
            model_type=model_type,
            table_name=", ".join(tables_processed),
            metadata_count=len(valid_models),
            warnings=warnings
        )
    
    def _metadata_to_dict(self, metadata_entry) -> Dict[str, Any]:
        """
        Convert metadata ORM object to dictionary
        """
        return {
            "id": metadata_entry.id,
            "column_name": metadata_entry.column_name,
            "data_type": metadata_entry.data_type,
            "description": metadata_entry.description,
            "business_definition": metadata_entry.business_definition,
            "is_primary_key": metadata_entry.is_primary_key,
            "is_foreign_key": metadata_entry.is_foreign_key,
            "foreign_key_table": metadata_entry.foreign_key_table,
            "foreign_key_column": metadata_entry.foreign_key_column,
            "nullable": metadata_entry.nullable,
            "source_system": metadata_entry.source_system
        }
    
    def _convert_metadata_to_df(self, metadata_entries) -> pd.DataFrame:
        """
        Convert metadata entries to pandas DataFrame with support for hierarchical structure
        """
        records = []
        for entry in metadata_entries:
            record = self._metadata_to_dict(entry)
            
            # Add schema information from additional_properties
            if entry.additional_properties and 'schema_name' in entry.additional_properties:
                record['schema_name'] = entry.additional_properties['schema_name']
            else:
                record['schema_name'] = 'default'
                
            # Add table description from additional_properties
            if entry.additional_properties and 'table_description' in entry.additional_properties:
                record['table_description'] = entry.additional_properties['table_description']
            
            records.append(record)
        
        return pd.DataFrame(records)
    
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
                'attributes': []
            }
            
            # Add attributes
            if entry.is_primary_key:
                column_info['attributes'].append('PK')
            
            if entry.is_foreign_key:
                column_info['attributes'].append(f"FK -> {entry.foreign_key_table}.{entry.foreign_key_column}")
            
            if not entry.nullable:
                column_info['attributes'].append('NOT NULL')
                
            if entry.description:
                column_info['description'] = entry.description
                
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
                    
                if 'description' in col:
                    col_str += f" - {col['description']}"
                    
                formatted_parts.append(col_str)
                
            formatted_parts.append("")  # Empty line between tables
        
        return "\n".join(formatted_parts)
    
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
    
    async def _perform_two_phase_analysis(self, metadata_content: str) -> Dict[str, Any]:
        """
        Perform two-phase analysis of metadata (Hub/Link -> Satellite)
        Only takes the formatted metadata content to save tokens
        """
        try:
            logger.debug(f"Starting two-phase analysis with metadata of length: {len(metadata_content)}")
            
            # Phase 1: Hub and Link Analysis
            hub_link_result = await self._analyze_hub_links(metadata_content)
            self.state.hub_link_analysis = hub_link_result
            
            # Phase 2: Satellite Analysis
            sat_result = await self._analyze_satellites(metadata_content, hub_link_result)
            self.state.satellite_analysis = sat_result
            
            # Combine results
            # Load results as Python dictionaries
            hub_link_data = json.loads(hub_link_result)
            sat_data = json.loads(sat_result)
            
            # Create combined result with proper structure matching YAML output format
            combined_result = {
                "hubs": hub_link_data.get("hubs", []),
                "links": hub_link_data.get("links", []),
                "satellites": sat_data.get("satellites", []),
                "link_satellites": sat_data.get("link_satellites", [])
            }
            self.state.final_analysis = combined_result
            
            logger.info("Two-phase analysis completed successfully")
            return combined_result
        except Exception as e:
            logger.error(f"Error in two-phase analysis: {str(e)}", exc_info=True)
            self.state.warnings.append(f"Error in model analysis: {str(e)}")
            return None
    
    async def _analyze_hub_links(self, metadata_content: str) -> str:
        """
        Phase 1: Analyze metadata for Hubs and Links
        Takes only the formatted metadata content
        """
        hub_link_prompt = """
    You are a Data Vault 2.0 modeling expert in banking domain.
    Analyze the table metadata given by user and recommend appropriate HUB and LINK components.
    TABLE METADATA:
    {metadata}
                
    Here are some requirements:
    A Hub component represent an unique business object, and a Link component represent a relationship between Hubs.
    A component can be derived from multiple source tables.
    A Link component must include at least 2 existed Hub components in relationships.
    Do NOT assume that a table is a Hub or Link component if it does not meet the requirements.

    Think step by step and response the final result with the following JSON format in a markdown cell:
    {{
        "hubs": [
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
        ]
    }}
    """.strip()
        
        try:
            # Call LLM with prompt only using the formatted metadata content
            result = await self._call_llm_with_prompt(hub_link_prompt, {"metadata": metadata_content})
            
            # Convert result back to JSON string
            return json.dumps(result)
        except Exception as e:
            logger.error(f"Error analyzing hubs and links: {str(e)}")
            raise
    
    async def _analyze_satellites(self, metadata_content: str, hub_link_result: str) -> str:
        """
        Phase 2: Analyze metadata for Satellites based on Hub/Link results
        Takes only the formatted metadata content and hub_link_result
        """
        sat_prompt = """
        You are a Data Vault 2.0 modeling expert tasked with designing Satellites.
        
        Based on the Hub and Link analysis result below, identify appropriate Satellites:
        
        HUB AND LINK ANALYSIS:
        {hub_link_result}
        
        TABLE METADATA:
        {metadata}
        
        For each Hub and Link, determine which descriptive attributes should be placed in Satellites:
        1. Group descriptive attributes by their rate of change and business context
        2. Non-key attributes typically belong in Satellites
        3. Create context-specific Satellites for different attribute groups
        
    Think step by step and response the final result with the following JSON format in a markdown cell:
    {{
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
    }}
        
        IMPORTANT RULES:
        - Satellite names must start with "SAT_" and have "target_entity_type": "sat"
        - Link Satellite names must start with "LSAT_" and have "target_entity_type": "LSAT"
        - Satellites must reference their parent Hub as "parent_table"
        - Satellites must include hash key columns (hash_key_sat, hash_key_hub, hash_diff)
        - Link Satellites must reference their parent Link as "parent_table"
        - Always include the business keys from the parent entity in the appropriate hash key
        """
        
        try:
            # Call LLM with prompt only using the necessary data
            result = await self._call_llm_with_prompt(
                sat_prompt, 
                {"metadata": metadata_content, "hub_link_result": hub_link_result}
            )
            
            # Convert result back to JSON string
            return json.dumps(result)
        except Exception as e:
            logger.error(f"Error analyzing satellites: {str(e)}")
            raise
    
    def validate_data_vault_model(self, model_data: Dict[str, Any]) -> List[str]:
        """
        Validate Data Vault model against best practices and rules
        Based on the new YAML structure format
        """
        warnings = []
        
        try:
            # Process and store hub metadata for cross-reference
            for hub in model_data.get("hubs", []):
                # Validate hub name format
                hub_name = hub.get("target_table", "")
                if not hub_name.startswith("HUB_"):
                    warnings.append(f"Hub name should start with 'HUB_': {hub_name}")
                
                # Check entity type
                if hub.get("target_entity_type") != "hub":
                    warnings.append(f"Hub {hub_name} has incorrect target_entity_type: {hub.get('target_entity_type')}")
                
                # Validate business keys by checking columns
                biz_keys = []
                for column in hub.get("columns", []):
                    if column.get("key_type") == "biz_key":
                        biz_keys.append(column.get("target"))
                
                if not biz_keys:
                    raise DataVaultValidationError(f"Business keys cannot be empty for hub: {hub_name}")
                
                # Store for cross-reference
                self.state.hubs_metadata[hub_name] = hub
            
            # Process and validate link metadata
            for link in model_data.get("links", []):
                # Validate link name format
                link_name = link.get("target_table", "")
                if not link_name.startswith("LNK_"):
                    warnings.append(f"Link name should start with 'LNK_': {link_name}")
                
                # Check entity type
                if link.get("target_entity_type") != "lnk":
                    warnings.append(f"Link {link_name} has incorrect target_entity_type: {link.get('target_entity_type')}")
                
                # Validate related hubs by checking parent references in columns
                related_hubs = []
                for column in link.get("columns", []):
                    if column.get("key_type") == "hash_key_hub" and column.get("parent"):
                        related_hubs.append(column.get("parent"))
                
                if len(related_hubs) < 2:
                    warnings.append(f"Link {link_name} must connect at least 2 Hubs. Found: {len(related_hubs)}")
                
                # Store for cross-reference
                self.state.links_metadata[link_name] = link
            
            # Process and validate satellite metadata
            for sat in model_data.get("satellites", []):
                # Validate satellite name format
                sat_name = sat.get("target_table", "")
                if not sat_name.startswith("SAT_"):
                    warnings.append(f"Satellite name should start with 'SAT_': {sat_name}")
                
                # Check entity type
                if sat.get("target_entity_type").lower() != "sat":
                    warnings.append(f"Satellite {sat_name} has incorrect target_entity_type: {sat.get('target_entity_type')}")
                
                # Validate hub existence
                parent_hub = sat.get("parent_table")
                if parent_hub not in self.state.hubs_metadata:
                    warnings.append(f"Satellite {sat_name} references non-existent hub: {parent_hub}")
                
                # Check for required columns
                has_hash_key_sat = False
                has_hash_key_hub = False
                has_hash_diff = False
                
                for column in sat.get("columns", []):
                    if column.get("key_type") == "hash_key_sat":
                        has_hash_key_sat = True
                    elif column.get("key_type") == "hash_key_hub":
                        has_hash_key_hub = True
                    elif column.get("key_type") == "hash_diff":
                        has_hash_diff = True
                
                if not has_hash_key_sat:
                    warnings.append(f"Satellite {sat_name} is missing hash_key_sat column")
                if not has_hash_key_hub:
                    warnings.append(f"Satellite {sat_name} is missing hash_key_hub column")
                if not has_hash_diff:
                    warnings.append(f"Satellite {sat_name} is missing hash_diff column")
                
                # Store for cross-reference
                self.state.sats_metadata[sat_name] = sat
            
            # Process and validate link satellite metadata
            for lsat in model_data.get("link_satellites", []):
                # Validate link satellite name format
                lsat_name = lsat.get("target_table", "")
                if not lsat_name.startswith("LSAT_"):
                    warnings.append(f"Link satellite name should start with 'LSAT_': {lsat_name}")
                
                # Check entity type - sometimes this is LSAT or lsat
                if lsat.get("target_entity_type").upper() != "LSAT":
                    warnings.append(f"Link satellite {lsat_name} has incorrect target_entity_type: {lsat.get('target_entity_type')}")
                
                # Validate link existence
                parent_link = lsat.get("parent_table")
                if parent_link not in self.state.links_metadata:
                    warnings.append(f"Link satellite {lsat_name} references non-existent link: {parent_link}")
                
                # Check for required columns
                has_hash_key_sat = False
                has_hash_key_link = False
                has_hash_diff = False
                
                for column in lsat.get("columns", []):
                    if column.get("key_type") == "hash_key_sat":
                        has_hash_key_sat = True
                    elif column.get("key_type") == "hash_key_hub" or column.get("key_type") == "hash_key_lnk":
                        has_hash_key_link = True
                    elif column.get("key_type") == "hash_diff":
                        has_hash_diff = True
                
                if not has_hash_key_sat:
                    warnings.append(f"Link satellite {lsat_name} is missing hash_key_sat column")
                if not has_hash_key_link:
                    warnings.append(f"Link satellite {lsat_name} is missing hash_key_hub/hash_key_lnk column")
                if not has_hash_diff:
                    warnings.append(f"Link satellite {lsat_name} is missing hash_diff column")
                
                # Store for cross-reference
                self.state.lsats_metadata[lsat_name] = lsat
            
            # Store warnings in state
            self.state.warnings = warnings
            
            return warnings
        except Exception as e:
            logger.error(f"Error validating Data Vault model: {str(e)}", exc_info=True)
            self.state.warnings.append(f"Validation error: {str(e)}")
            return self.state.warnings
    

