"""
LLM Service for Knowledge Graph.
This service provides functionality to integrate LLM capabilities with the knowledge graph.
"""
import time
import json
from typing import Dict, Any, List, Optional, Tuple
import re

from app.core.logging import logger
from app.core.config import settings
from app.knowledge_graph.services.graph_connector import GraphConnector
import asyncio
import httpx
from pydantic import BaseModel


class LLMService:
    """Service for LLM integration with the knowledge graph"""
    
    def __init__(self):
        """Initialize the LLM service"""
        self.graph = GraphConnector()
        self.provider = settings.LLM_PROVIDER
        self.model = settings.LLM_MODEL
        
        # Cache for schema information
        self._schema_cache = None
    
    async def natural_language_to_cypher(self, query: str) -> Dict[str, Any]:
        """
        Convert natural language query to Cypher query
        
        Args:
            query: Natural language query
            
        Returns:
            Dict containing generated Cypher query and results
        """
        start_time = time.time()
        
        # Get schema information if not cached
        if self._schema_cache is None:
            self._schema_cache = await self._get_schema_information()
        
        schema_info = self._schema_cache
        
        # Prepare prompt with schema information
        prompt = self._prepare_cypher_prompt(query, schema_info)
        
        # Call LLM API
        generated_cypher = await self._call_llm_api(prompt)
        
        # Extract Cypher query from response
        cypher_query = self._extract_cypher_query(generated_cypher)
        
        # Execute the Cypher query if valid
        results = []
        error_message = None
        if cypher_query:
            try:
                results = self.graph.execute_cypher(cypher_query)
            except Exception as e:
                error_message = str(e)
                logger.error(f"Error executing generated Cypher query: {str(e)}")
        
        execution_time = time.time() - start_time
        logger.info(f"Natural language to Cypher conversion completed in {execution_time:.2f} seconds")
        
        return {
            "message": "Natural language query executed",
            "original_query": query,
            "generated_cypher": cypher_query,
            "results": results,
            "error": error_message,
            "execution_time": execution_time
        }
    
    async def _get_schema_information(self) -> Dict[str, Any]:
        """
        Get schema information from the graph database
        
        Returns:
            Dict containing schema information
        """
        logger.info("Retrieving schema information from graph database")
        
        # Get node labels
        labels_query = """
        CALL db.labels() YIELD label
        RETURN collect(label) as labels
        """
        labels_result = self.graph.execute_cypher(labels_query)
        labels = labels_result[0]["labels"] if labels_result else []
        
        # Get relationship types
        rel_types_query = """
        CALL db.relationshipTypes() YIELD relationshipType
        RETURN collect(relationshipType) as relationship_types
        """
        rel_types_result = self.graph.execute_cypher(rel_types_query)
        relationship_types = rel_types_result[0]["relationship_types"] if rel_types_result else []
        
        # Get property keys for each node label
        node_properties = {}
        for label in labels:
            properties_query = f"""
            MATCH (n:{label})
            UNWIND keys(n) AS key
            RETURN collect(DISTINCT key) AS property_keys
            LIMIT 1
            """
            properties_result = self.graph.execute_cypher(properties_query)
            property_keys = properties_result[0]["property_keys"] if properties_result else []
            node_properties[label] = property_keys
        
        # Get sample nodes for each label
        sample_nodes = {}
        for label in labels:
            sample_query = f"""
            MATCH (n:{label})
            RETURN n LIMIT 1
            """
            sample_result = self.graph.execute_cypher(sample_query)
            if sample_result:
                sample_nodes[label] = dict(sample_result[0].get("n", {}))
        
        # Return combined schema information
        return {
            "labels": labels,
            "relationship_types": relationship_types,
            "node_properties": node_properties,
            "sample_nodes": sample_nodes
        }
    
    def _prepare_cypher_prompt(self, query: str, schema_info: Dict[str, Any]) -> str:
        """
        Prepare prompt for LLM to generate Cypher query
        
        Args:
            query: Natural language query
            schema_info: Graph schema information
            
        Returns:
            Prompt for LLM
        """
        # Prepare node labels and properties information
        labels_info = ""
        for label, properties in schema_info["node_properties"].items():
            labels_info += f"- {label}: "
            if properties:
                labels_info += ", ".join(properties)
            labels_info += "\n"
        
        # Prepare relationship types information
        rel_types_info = ", ".join(schema_info["relationship_types"])
        
        # Build the prompt
        prompt = f"""
You are a Cypher query generator for a Neo4j graph database.
Convert the following natural language question into a Cypher query.

The graph contains these node labels:
{labels_info}

And these relationship types:
{rel_types_info}

Some examples of common patterns:
1. Finding nodes by property: MATCH (n:Label {{property: value}}) RETURN n
2. Find connected nodes: MATCH (n:Label1)-[r:RELATIONSHIP_TYPE]->(m:Label2) RETURN n, r, m
3. Find paths between nodes: MATCH path = (start:Label1)-[*..5]->(end:Label2) RETURN path

Natural language query: {query}

Generate a valid Cypher query that answers this question. 
Only return the Cypher query without any explanations.
"""
        
        return prompt
    
    async def _call_llm_api(self, prompt: str) -> str:
        """
        Call LLM API to generate response
        
        Args:
            prompt: Input prompt for LLM
            
        Returns:
            Generated response
        """
        try:
            if self.provider == "openai":
                return await self._call_openai_api(prompt)
            elif self.provider == "groq":
                return await self._call_groq_api(prompt)
            else:
                raise ValueError(f"Unsupported LLM provider: {self.provider}")
        except Exception as e:
            logger.error(f"Error calling LLM API: {str(e)}")
            return "Failed to generate Cypher query due to API error."
    
    async def _call_openai_api(self, prompt: str) -> str:
        """
        Call OpenAI API
        
        Args:
            prompt: Input prompt
            
        Returns:
            Generated response
        """
        if not settings.OPENAI_API_KEY:
            raise ValueError("OpenAI API key not configured")
        
        api_key = settings.OPENAI_API_KEY
        model = "gpt-4"  # Default to GPT-4 for better reasoning
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}"
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "You are a Cypher query expert."},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.2
                },
                timeout=60.0
            )
            
            if response.status_code != 200:
                raise Exception(f"OpenAI API error: {response.text}")
            
            data = response.json()
            return data["choices"][0]["message"]["content"]
    
    async def _call_groq_api(self, prompt: str) -> str:
        """
        Call Groq API
        
        Args:
            prompt: Input prompt
            
        Returns:
            Generated response
        """
        if not settings.GROQ_API_KEY:
            raise ValueError("Groq API key not configured")
        
        api_key = settings.GROQ_API_KEY
        model = settings.LLM_MODEL  # Use configured model
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}"
                },
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": "You are a Cypher query expert."},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.2
                },
                timeout=60.0
            )
            
            if response.status_code != 200:
                raise Exception(f"Groq API error: {response.text}")
            
            data = response.json()
            return data["choices"][0]["message"]["content"]
    
    def _extract_cypher_query(self, response: str) -> Optional[str]:
        """
        Extract Cypher query from LLM response
        
        Args:
            response: LLM response
            
        Returns:
            Extracted Cypher query or None if not found
        """
        # Try to extract query from code blocks
        code_block_pattern = r"```(?:cypher)?\s*([\s\S]*?)```"
        code_blocks = re.findall(code_block_pattern, response)
        
        if code_blocks:
            return code_blocks[0].strip()
        
        # If no code blocks, try to extract the whole response if it looks like a Cypher query
        response = response.strip()
        if response.upper().startswith("MATCH") or response.upper().startswith("CALL"):
            return response
        
        return None
