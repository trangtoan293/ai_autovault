"""
Enhanced Data Vault 2.0 model generation with database storage
Combines automatic and manual input approaches with unified storage
"""
from typing import List, Dict, Any
from fastapi import HTTPException
from sqlalchemy.orm import Session
from app.core.config import settings
from app.core.logging import logger
from app.models.response import ModelGenerationResponse, ModelConfig
from app.models.data_vault import (
    ManualComponentInput, HubComponent, LinkComponent, 
    SatelliteComponent, LinkSatelliteComponent, SimpleManualInput
)
from app.services.data_vault_store import DataVaultStoreService
from app.services.model_generator import ModelGeneratorService, DataVaultModelState


class EnhancedModelGeneratorService(ModelGeneratorService):
    """Enhanced service for Data Vault model generation with database storage"""
    
    def __init__(self):
        super().__init__()
        # Initialize data vault store service for saving models
        self.data_vault_store = DataVaultStoreService()
    
    async def generate_models_from_source(self, source_system: str, table_name: str, db: Session) -> ModelGenerationResponse:
        """
        Generate Data Vault models based on source system and table name.
        Extends the original method to save components to database.
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
                                
                                # Save to database
                                hub_component = HubComponent(
                                    name=hub.get('name'),
                                    description=hub.get('description'),
                                    business_keys=hub.get('business_keys', []),
                                    source_tables=hub.get('source_tables', []),
                                    source_schema=source_system,
                                    source_table=table_name,
                                    source_columns=hub.get('source_columns', []),
                                    target_schema=hub.get('target_schema', settings.DEFAULT_TARGET_SCHEMA),
                                    target_table=hub.get('name'),
                                    target_columns=hub.get('target_columns', []),
                                    collision_code=hub.get('collision_code', settings.DEFAULT_COLLISION_CODE),
                                    lineage_mapping=hub.get('lineage_mapping'),
                                    transformation_logic=hub.get('transformation_logic'),
                                    yaml_content=model_yaml
                                )
                                self.data_vault_store.save_component(
                                    db, 
                                    hub_component, 
                                    source_system, 
                                    source_schema=source_system,
                                    source_table=table_name,
                                    target_schema=hub.get('target_schema', settings.DEFAULT_TARGET_SCHEMA),
                                    collision_code=hub.get('collision_code', settings.DEFAULT_COLLISION_CODE)
                                )
                                logger.debug(f"Saved hub component to database: {hub.get('name', 'unknown')}")
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
                                
                                # Save to database
                                link_component = LinkComponent(
                                    name=link.get('name'),
                                    description=link.get('description'),
                                    business_keys=link.get('business_keys', []),
                                    source_tables=link.get('source_tables', []),
                                    source_schema=source_system,
                                    source_table=table_name,
                                    source_columns=link.get('source_columns', []),
                                    related_hubs=link.get('related_hubs', []),
                                    target_schema=link.get('target_schema', settings.DEFAULT_TARGET_SCHEMA),
                                    target_table=link.get('name'),
                                    target_columns=link.get('target_columns', []),
                                    collision_code=link.get('collision_code', settings.DEFAULT_COLLISION_CODE),
                                    lineage_mapping=link.get('lineage_mapping'),
                                    transformation_logic=link.get('transformation_logic'),
                                    yaml_content=model_yaml
                                )
                                self.data_vault_store.save_component(
                                    db, 
                                    link_component, 
                                    source_system, 
                                    source_schema=source_system,
                                    source_table=table_name,
                                    target_schema=link.get('target_schema', settings.DEFAULT_TARGET_SCHEMA),
                                    collision_code=link.get('collision_code', settings.DEFAULT_COLLISION_CODE)
                                )
                                logger.debug(f"Saved link component to database: {link.get('name', 'unknown')}")
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
                                
                                # Save to database
                                sat_component = SatelliteComponent(
                                    name=sat.get('name'),
                                    description=f"Satellite for {sat.get('hub')}",
                                    business_keys=sat.get('business_keys', []),
                                    hub=sat.get('hub'),
                                    source_tables=[sat.get('source_table', table_name)],
                                    source_schema=source_system,
                                    source_table=sat.get('source_table', table_name),
                                    source_columns=[],
                                    target_schema=sat.get('target_schema', settings.DEFAULT_TARGET_SCHEMA),
                                    target_table=sat.get('name'),
                                    target_columns=sat.get('target_columns', []),
                                    collision_code=sat.get('collision_code', settings.DEFAULT_COLLISION_CODE),
                                    lineage_mapping=sat.get('lineage_mapping'),
                                    transformation_logic=sat.get('transformation_logic'),
                                    yaml_content=model_yaml
                                )
                                self.data_vault_store.save_component(
                                    db, 
                                    sat_component, 
                                    source_system, 
                                    source_schema=source_system,
                                    source_table=sat.get('source_table', table_name),
                                    target_schema=sat.get('target_schema', settings.DEFAULT_TARGET_SCHEMA),
                                    collision_code=sat.get('collision_code', settings.DEFAULT_COLLISION_CODE)
                                )
                                logger.debug(f"Saved satellite component to database: {sat.get('name', 'unknown')}")
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
                                
                                # Save to database
                                lsat_component = LinkSatelliteComponent(
                                    name=lsat.get('name'),
                                    description=f"Link Satellite for {lsat.get('link')}",
                                    business_keys=lsat.get('business_keys', []),
                                    link=lsat.get('link'),
                                    source_tables=[lsat.get('source_table', table_name)],
                                    source_schema=source_system,
                                    source_table=lsat.get('source_table', table_name),
                                    source_columns=[],
                                    target_schema=lsat.get('target_schema', settings.DEFAULT_TARGET_SCHEMA),
                                    target_table=lsat.get('name'),
                                    target_columns=lsat.get('target_columns', []),
                                    collision_code=lsat.get('collision_code', settings.DEFAULT_COLLISION_CODE),
                                    lineage_mapping=lsat.get('lineage_mapping'),
                                    transformation_logic=lsat.get('transformation_logic'),
                                    yaml_content=model_yaml
                                )
                                self.data_vault_store.save_component(
                                    db, 
                                    lsat_component, 
                                    source_system, 
                                    source_schema=source_system,
                                    source_table=lsat.get('source_table', table_name),
                                    target_schema=lsat.get('target_schema', settings.DEFAULT_TARGET_SCHEMA),
                                    collision_code=lsat.get('collision_code', settings.DEFAULT_COLLISION_CODE)
                                )
                                logger.debug(f"Saved link satellite component to database: {lsat.get('name', 'unknown')}")
                        except Exception as e:
                            logger.error(f"Error processing link satellite {lsat.get('name', 'unknown')}: {str(e)}", exc_info=True)
                            self.state.warnings.append(f"Error processing link satellite {lsat.get('name', 'unknown')}: {str(e)}")
                
            # Get the saved model components
            saved_components = self.data_vault_store.get_components_by_source_and_table(
                db, source_system, table_name
            )
            logger.info(f"Saved {len(saved_components)} components to database")
            
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
                model_yaml=combined_yaml,
                table_name=table_name,
                model_type="auto",
                metadata_count=len(metadata_entries),
                warnings=self.state.warnings
            )
            return model_result

        except Exception as e:
            logger.error(f"Error generating model from source: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error generating model from source: {str(e)}")
        

    async def generate_models_from_simple_input(
        self, 
        input_data: SimpleManualInput, 
        source_system: str,
        table_name: str,
        db: Session
    ) -> ModelGenerationResponse:
        """
        Generate Data Vault models from a simple input format (similar to LLM output)
        This method is designed to work with input format that matches the LLM result structure
        """
        try:
            logger.info(f"Generating models from simple input format for {table_name}")
            
            # Get metadata for the specified source system and table
            metadata_entries = self.metadata_service.get_metadata_by_source_and_table(
                db, source_system, table_name
            )
            
            if not metadata_entries:
                logger.warning(f"No metadata found for source system {source_system} and table {table_name}")
                self.state.warnings.append(f"No metadata found for source system {source_system} and table {table_name}. Column details may be incomplete.")
            
            # Generate YAML for each component type
            generated_models = []
            
            # Process hubs
            if input_data.hubs:
                for hub in input_data.hubs:
                    try:
                        logger.debug(f"Processing hub: {hub.name}")
                        hub_yaml = self._generate_hub_yaml(
                            hub.dict(), 
                            source_system, 
                            table_name, 
                            metadata_entries or []
                        )
                        
                        if hub_yaml:
                            generated_models.append(hub_yaml)
                            logger.debug(f"Generated YAML for hub: {hub.name}")
                            
                            # Convert to HubComponent for database storage
                            hub_component = HubComponent(
                                name=hub.name,
                                description=hub.description or f"Hub for {hub.name}",
                                business_keys=hub.business_keys,
                                source_tables=hub.source_tables,
                                source_schema=hub.source_schema or source_system,
                                source_table=hub.source_table or table_name,
                                source_columns=hub.source_columns or [],
                                target_schema=hub.target_schema,
                                target_table=hub.target_table or hub.name,
                                target_columns=hub.target_columns or [],
                                collision_code=hub.collision_code,
                                lineage_mapping=hub.lineage_mapping,
                                transformation_logic=hub.transformation_logic,
                                yaml_content=hub_yaml
                            )
                            
                            # Save to database
                            self.data_vault_store.save_component(
                                db, 
                                hub_component, 
                                source_system, 
                                source_schema=hub.source_schema or source_system,
                                source_table=hub.source_table or table_name,
                                target_schema=hub.target_schema,
                                collision_code=hub.collision_code
                            )
                            logger.debug(f"Saved hub component to database: {hub.name}")
                            
                    except Exception as e:
                        logger.error(f"Error processing hub {hub.name}: {str(e)}", exc_info=True)
                        self.state.warnings.append(f"Error processing hub {hub.name}: {str(e)}")
            
            # Process links
            if input_data.links:
                for link in input_data.links:
                    try:
                        logger.debug(f"Processing link: {link.name}")
                        link_yaml = self._generate_link_yaml(
                            link.dict(), 
                            source_system, 
                            table_name, 
                            metadata_entries or []
                        )
                        
                        if link_yaml:
                            generated_models.append(link_yaml)
                            logger.debug(f"Generated YAML for link: {link.name}")
                            
                            # Convert to LinkComponent for database storage
                            link_component = LinkComponent(
                                name=link.name,
                                description=link.description or f"Link for {link.name}",
                                business_keys=link.business_keys,
                                source_tables=link.source_tables,
                                source_schema=link.source_schema or source_system,
                                source_table=link.source_table or table_name,
                                source_columns=link.source_columns or [],
                                related_hubs=link.related_hubs,
                                target_schema=link.target_schema,
                                target_table=link.target_table or link.name,
                                target_columns=link.target_columns or [],
                                collision_code=link.collision_code,
                                lineage_mapping=link.lineage_mapping,
                                transformation_logic=link.transformation_logic,
                                yaml_content=link_yaml
                            )
                            
                            # Save to database
                            self.data_vault_store.save_component(
                                db, 
                                link_component, 
                                source_system, 
                                source_schema=link.source_schema or source_system,
                                source_table=link.source_table or table_name,
                                target_schema=link.target_schema,
                                collision_code=link.collision_code
                            )
                            logger.debug(f"Saved link component to database: {link.name}")
                            
                    except Exception as e:
                        logger.error(f"Error processing link {link.name}: {str(e)}", exc_info=True)
                        self.state.warnings.append(f"Error processing link {link.name}: {str(e)}")
            
            # Process satellites
            if input_data.satellites:
                for sat in input_data.satellites:
                    try:
                        logger.debug(f"Processing satellite: {sat.name}")
                        satellite_yaml = self._generate_satellite_yaml(
                            sat.dict(), 
                            source_system, 
                            table_name, 
                            metadata_entries or []
                        )
                        
                        if satellite_yaml:
                            generated_models.append(satellite_yaml)
                            logger.debug(f"Generated YAML for satellite: {sat.name}")
                            
                            # Convert to SatelliteComponent for database storage
                            sat_component = SatelliteComponent(
                                name=sat.name,
                                description=f"Satellite for {sat.hub}",
                                business_keys=sat.business_keys,
                                hub=sat.hub,
                                source_tables=[sat.source_table],
                                source_schema=sat.source_schema or source_system,
                                source_table=sat.source_table,
                                source_columns=sat.source_columns ,
                                target_schema=sat.target_schema,
                                target_table=sat.target_table or sat.name,
                                target_columns=sat.target_columns or [],
                                collision_code=sat.collision_code,
                                lineage_mapping=sat.lineage_mapping,
                                transformation_logic=sat.transformation_logic,
                                yaml_content=satellite_yaml
                            )
                            
                            # Save to database
                            self.data_vault_store.save_component(
                                db, 
                                sat_component, 
                                source_system, 
                                source_schema=sat.source_schema or source_system,
                                source_table=sat.source_table,
                                target_schema=sat.target_schema,
                                collision_code=sat.collision_code
                            )
                            logger.debug(f"Saved satellite component to database: {sat.name}")
                            
                    except Exception as e:
                        logger.error(f"Error processing satellite {sat.name}: {str(e)}", exc_info=True)
                        self.state.warnings.append(f"Error processing satellite {sat.name}: {str(e)}")
            
            # Process link satellites
            if input_data.link_satellites:
                for lsat in input_data.link_satellites:
                    try:
                        logger.debug(f"Processing link satellite: {lsat.name}")
                        link_satellite_yaml = self._generate_link_satellite_yaml(
                            lsat.dict(), 
                            source_system, 
                            table_name, 
                            metadata_entries or []
                        )
                        
                        if link_satellite_yaml:
                            generated_models.append(link_satellite_yaml)
                            logger.debug(f"Generated YAML for link satellite: {lsat.name}")
                            
                            # Convert to LinkSatelliteComponent for database storage
                            lsat_component = LinkSatelliteComponent(
                                name=lsat.name,
                                description=f"Link Satellite for {lsat.link}",
                                business_keys=lsat.business_keys,
                                link=lsat.link,
                                source_tables=[lsat.source_table],
                                source_schema=lsat.source_schema or source_system,
                                source_table=lsat.source_table,
                                source_columns=lsat.source_columns ,
                                target_schema=lsat.target_schema,
                                target_table=lsat.target_table or lsat.name,
                                target_columns=lsat.target_columns or [],
                                collision_code=lsat.collision_code,
                                lineage_mapping=lsat.lineage_mapping,
                                transformation_logic=lsat.transformation_logic,
                                yaml_content=link_satellite_yaml
                            )
                            
                            # Save to database
                            self.data_vault_store.save_component(
                                db, 
                                lsat_component, 
                                source_system, 
                                source_schema=lsat.source_schema or source_system,
                                source_table=lsat.source_table,
                                target_schema=lsat.target_schema,
                                collision_code=lsat.collision_code
                            )
                            logger.debug(f"Saved link satellite component to database: {lsat.name}")
                            
                    except Exception as e:
                        logger.error(f"Error processing link satellite {lsat.name}: {str(e)}", exc_info=True)
                        self.state.warnings.append(f"Error processing link satellite {lsat.name}: {str(e)}")
            
            # If no models were generated, return a message
            if not generated_models:
                logger.warning(f"No models could be generated for {table_name}")
                return ModelGenerationResponse(
                    message="ERROR",
                    model_yaml=f"# No models could be generated for {table_name}",
                    table_name=table_name,
                    model_type="simple",
                    metadata_count=len(metadata_entries) if metadata_entries else 0,
                    warnings=self.state.warnings
                )
            
            # Combine all generated models
            combined_yaml = " --- ".join(generated_models)
            
            logger.info(f"Generated {len(generated_models)} models for {table_name} from simple input")
            
            model_result = ModelGenerationResponse(
                message="DONE",
                model_yaml=combined_yaml,
                table_name=table_name,
                model_type="simple",
                metadata_count=len(metadata_entries) if metadata_entries else 0,
                warnings=self.state.warnings
            )
            return model_result

        except Exception as e:
            logger.error(f"Error generating models from simple input: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Error generating models from simple input: {str(e)}")

    def render_data_to_json(self, components) -> Dict[str, List[Dict[str, Any]]]:
        """
        Render Data Vault components to JSON format
        return:
            {
                "hub":[
                    "id": id,
                    "name": name,
                    "business_keys": business_keys,
                    "source_tables": source_tables,
                    "description": description,
                    "yaml_content": yaml_content
                    ],
                "link":[],"satellite":[],"link_satellite":[]
        """
        result = {
            "hubs": [],
            "links": [],
            "satellites": [],
            "link_satellites": []
        }
        
        for component in components:
            if component.component_type == "hub":
                result["hubs"].append({
                    "id": component.id,
                    "name": component.name,
                    "business_keys": component.business_keys,
                    "source_tables": component.source_tables,
                    "source_schema": component.source_schema,
                    "source_table": component.source_table or component.table_name,
                    "source_columns": component.source_columns,
                    "target_schema": component.target_schema,
                    "target_table": component.target_table or component.name,
                    "target_columns": component.target_columns,
                    "collision_code": component.collision_code,
                    "lineage_mapping": component.lineage_mapping,
                    "transformation_logic": component.transformation_logic,
                    "description": component.description,
                    "yaml_content": component.yaml_content
                })
            elif component.component_type == "link":
                result["links"].append({
                    "id": component.id,
                    "name": component.name,
                    "business_keys": component.business_keys,
                    "source_tables": component.source_tables,
                    "source_schema": component.source_schema,
                    "source_table": component.source_table or component.table_name,
                    "source_columns": component.source_columns,
                    "related_hubs": component.related_hubs,
                    "target_schema": component.target_schema,
                    "target_table": component.target_table or component.name,
                    "target_columns": component.target_columns,
                    "collision_code": component.collision_code,
                    "lineage_mapping": component.lineage_mapping,
                    "transformation_logic": component.transformation_logic,
                    "description": component.description,
                    "yaml_content": component.yaml_content
                })
            elif component.component_type == "satellite":
                result["satellites"].append({
                    "id": component.id,
                    "name": component.name,
                    "business_keys": component.business_keys,
                    "hub": component.hub_name,
                    "source_tables": component.source_tables,
                    "source_schema": component.source_schema,
                    "source_table": component.source_table or component.table_name,
                    "source_columns": component.source_columns,
                    "target_schema": component.target_schema,
                    "target_table": component.target_table or component.name,
                    "target_columns": component.target_columns,
                    "collision_code": component.collision_code,
                    "lineage_mapping": component.lineage_mapping,
                    "transformation_logic": component.transformation_logic,
                    "description": component.description,
                    "yaml_content": component.yaml_content
                })
            elif component.component_type == "link_satellite":
                result["link_satellites"].append({
                    "id": component.id,
                    "name": component.name,
                    "business_keys": component.business_keys,
                    "link": component.link_name,
                    "source_tables": component.source_tables,
                    "source_schema": component.source_schema,
                    "source_table": component.source_table or component.table_name,
                    "source_columns": component.source_columns,
                    "target_schema": component.target_schema,
                    "target_table": component.target_table or component.name,
                    "target_columns": component.target_columns,
                    "collision_code": component.collision_code,
                    "lineage_mapping": component.lineage_mapping,
                    "transformation_logic": component.transformation_logic,
                    "description": component.description,
                    "yaml_content": component.yaml_content
                })
        
        return result

    def get_data_model_by_source_table(self, db: Session, source_system: str, table_name: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get saved models for a specific source system and table
        """
        components = self.data_vault_store.get_components_by_source_and_table(db, source_system, table_name)
        return self.render_data_to_json(components)
    
    def get_data_model_all(self,db:Session)->Dict[str, List[Dict[str, Any]]]:
        """
        Get all saved models
        """
        components = self.data_vault_store.get_all_components(db)
        return self.render_data_to_json(components)
    
    def get_data_model_by_type(self, db: Session, component_type: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get saved models by component type
        """
        components = self.data_vault_store.get_components_by_type(db, component_type)
        result= self.render_data_to_json(components)
        # Filter result to only include the requested component type
        if component_type == "hub":
            return {"hubs": result["hubs"]}
        elif component_type == "link":
            return {"links": result["links"]}
        elif component_type == "satellite":
            return {"satellites": result["satellites"]}
        elif component_type == "link_satellite":
            return {"link_satellites": result["link_satellites"]}
