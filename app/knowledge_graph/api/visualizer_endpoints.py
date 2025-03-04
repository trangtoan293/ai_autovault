"""
Visualization endpoints for the Knowledge Graph module.
These endpoints provide visualization capabilities for the knowledge graph.
"""
from fastapi import APIRouter, Depends, HTTPException, status, Query, Path as FastAPIPath, Response
from fastapi.responses import HTMLResponse, FileResponse
from typing import Dict, Any, Optional
import os
import tempfile
from pathlib import Path as FilePath

from app.core.logging import logger
from app.core.security import get_current_active_user, User
from app.knowledge_graph.utils.graph_visualizer import GraphVisualizer


router = APIRouter()


@router.get("/lineage/table/{table_name}", response_class=HTMLResponse)
async def visualize_table_lineage(
    table_name: str = FastAPIPath(..., description="Name of the table"),
    schema_name: Optional[str] = Query(None, description="Optional schema name"),
    direction: str = Query("both", description="Direction of lineage", enum=["upstream", "downstream", "both"]),
    current_user: User = Depends(get_current_active_user)
):
    """
    Generate an interactive visualization of table-level lineage
    
    Args:
        table_name: Name of the table
        schema_name: Optional schema name
        direction: Direction of lineage (upstream, downstream, both)
        current_user: Current authenticated user
        
    Returns:
        HTML visualization of the lineage
    """
    logger.info(f"User {current_user.username} requested table lineage visualization for {table_name}")
    
    try:
        # Generate the Cypher queries for nodes and relationships
        table_filter = f"WHERE n.name = '{table_name}'" + (f" AND n.schema = '{schema_name}'" if schema_name else "")
        
        # Define query based on direction
        if direction == "upstream":
            # Sources that flow into this table
            node_query = f"""
            MATCH path = (source:Table)-[:SOURCE_OF|CONTAINS*1..5]->(target:Table)
            {table_filter} AND target:Table
            UNWIND nodes(path) AS n
            RETURN DISTINCT n, labels(n) as labels, id(n) as id
            """
            
            relationship_query = f"""
            MATCH path = (source:Table)-[:SOURCE_OF|CONTAINS*1..5]->(target:Table)
            {table_filter} AND target:Table
            UNWIND relationships(path) AS r
            RETURN DISTINCT r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
            """
        
        elif direction == "downstream":
            # Targets that this table flows into
            node_query = f"""
            MATCH path = (source:Table)-[:SOURCE_OF|CONTAINS*1..5]->(target:Table)
            {table_filter} AND source:Table
            UNWIND nodes(path) AS n
            RETURN DISTINCT n, labels(n) as labels, id(n) as id
            """
            
            relationship_query = f"""
            MATCH path = (source:Table)-[:SOURCE_OF|CONTAINS*1..5]->(target:Table)
            {table_filter} AND source:Table
            UNWIND relationships(path) AS r
            RETURN DISTINCT r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
            """
        
        else:  # both
            # Both upstream and downstream
            node_query = f"""
            MATCH path = (node:Table)-[:SOURCE_OF|CONTAINS*1..5]-(other:Table)
            {table_filter} AND node:Table
            UNWIND nodes(path) AS n
            RETURN DISTINCT n, labels(n) as labels, id(n) as id
            """
            
            relationship_query = f"""
            MATCH path = (node:Table)-[:SOURCE_OF|CONTAINS*1..5]-(other:Table)
            {table_filter} AND node:Table
            UNWIND relationships(path) AS r
            RETURN DISTINCT r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
            """
        
        # Generate the visualization
        visualizer = GraphVisualizer()
        result = visualizer.generate_d3_visualization(
            node_query=node_query,
            relationship_query=relationship_query
        )
        
        # Return the HTML
        return HTMLResponse(content=result["html"], status_code=200)
    
    except Exception as e:
        logger.error(f"Error generating lineage visualization: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating lineage visualization: {str(e)}"
        )


@router.get("/lineage/column/{table_name}/{column_name}", response_class=HTMLResponse)
async def visualize_column_lineage(
    table_name: str = FastAPIPath(..., description="Name of the table"),
    column_name: str = FastAPIPath(..., description="Name of the column"),
    schema_name: Optional[str] = Query(None, description="Optional schema name"),
    direction: str = Query("both", description="Direction of lineage", enum=["upstream", "downstream", "both"]),
    current_user: User = Depends(get_current_active_user)
):
    """
    Generate an interactive visualization of column-level lineage
    
    Args:
        table_name: Name of the table
        column_name: Name of the column
        schema_name: Optional schema name
        direction: Direction of lineage (upstream, downstream, both)
        current_user: Current authenticated user
        
    Returns:
        HTML visualization of the lineage
    """
    logger.info(f"User {current_user.username} requested column lineage visualization for {table_name}.{column_name}")
    
    try:
        # Generate the Cypher queries for nodes and relationships
        column_filter = f"WHERE n.name = '{column_name}' AND n.table = '{table_name}'" + (f" AND n.schema = '{schema_name}'" if schema_name else "")
        
        # Define query based on direction
        if direction == "upstream":
            # Sources that flow into this column
            node_query = f"""
            MATCH path = (source:Column)-[:REFERENCES|MAPPED_TO|DERIVED_FROM*1..10]->(target:Column)
            {column_filter} AND target:Column
            UNWIND nodes(path) AS n
            RETURN DISTINCT n, labels(n) as labels, id(n) as id
            """
            
            relationship_query = f"""
            MATCH path = (source:Column)-[:REFERENCES|MAPPED_TO|DERIVED_FROM*1..10]->(target:Column)
            {column_filter} AND target:Column
            UNWIND relationships(path) AS r
            RETURN DISTINCT r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
            """
        
        elif direction == "downstream":
            # Targets that this column flows into
            node_query = f"""
            MATCH path = (source:Column)-[:REFERENCES|MAPPED_TO|DERIVED_FROM*1..10]->(target:Column)
            {column_filter} AND source:Column
            UNWIND nodes(path) AS n
            RETURN DISTINCT n, labels(n) as labels, id(n) as id
            """
            
            relationship_query = f"""
            MATCH path = (source:Column)-[:REFERENCES|MAPPED_TO|DERIVED_FROM*1..10]->(target:Column)
            {column_filter} AND source:Column
            UNWIND relationships(path) AS r
            RETURN DISTINCT r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
            """
        
        else:  # both
            # Both upstream and downstream
            node_query = f"""
            MATCH path = (node:Column)-[:REFERENCES|MAPPED_TO|DERIVED_FROM*1..10]-(other:Column)
            {column_filter} AND node:Column
            UNWIND nodes(path) AS n
            RETURN DISTINCT n, labels(n) as labels, id(n) as id
            """
            
            relationship_query = f"""
            MATCH path = (node:Column)-[:REFERENCES|MAPPED_TO|DERIVED_FROM*1..10]-(other:Column)
            {column_filter} AND node:Column
            UNWIND relationships(path) AS r
            RETURN DISTINCT r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
            """
        
        # Also include the table nodes for context
        table_context_query = f"""
        MATCH (c:Column)-[:CONTAINS]-(t:Table)
        WITH c, t
        WHERE c IN 
        (
            MATCH path = (node:Column)-[:REFERENCES|MAPPED_TO|DERIVED_FROM*1..10]-(other:Column)
            {column_filter} AND node:Column
            UNWIND nodes(path) AS n
            WHERE n:Column
            RETURN n
        )
        RETURN DISTINCT t, labels(t) as labels, id(t) as id
        """
        
        # Generate the visualization
        visualizer = GraphVisualizer()
        result = visualizer.generate_d3_visualization(
            node_query=node_query + " UNION " + table_context_query,
            relationship_query=relationship_query
        )
        
        # Return the HTML
        return HTMLResponse(content=result["html"], status_code=200)
    
    except Exception as e:
        logger.error(f"Error generating column lineage visualization: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating column lineage visualization: {str(e)}"
        )


@router.get("/data-vault/{component_type}", response_class=HTMLResponse)
async def visualize_data_vault_components(
    component_type: str = FastAPIPath(..., description="Type of Data Vault component", enum=["hub", "link", "satellite", "all"]),
    current_user: User = Depends(get_current_active_user)
):
    """
    Generate an interactive visualization of Data Vault components
    
    Args:
        component_type: Type of Data Vault component
        current_user: Current authenticated user
        
    Returns:
        HTML visualization of the components
    """
    logger.info(f"User {current_user.username} requested Data Vault visualization for {component_type} components")
    
    try:
        # Generate the Cypher queries for nodes and relationships
        if component_type == "all":
            # Get all Data Vault components
            node_query = """
            MATCH (n:DataVaultComponent)
            RETURN n, labels(n) as labels, id(n) as id
            """
        else:
            # Get specific type of Data Vault components
            node_query = f"""
            MATCH (n:DataVaultComponent)
            WHERE n.component_type = '{component_type}'
            RETURN n, labels(n) as labels, id(n) as id
            """
        
        # Get relationships between Data Vault components and to source tables
        relationship_query = """
        MATCH (n:DataVaultComponent)-[r]-(m)
        RETURN r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
        """
        
        # Also get source tables
        source_tables_query = """
        MATCH (n:DataVaultComponent)-[r]-(m:Table)
        RETURN m, labels(m) as labels, id(m) as id
        """
        
        # Generate the visualization
        visualizer = GraphVisualizer()
        result = visualizer.generate_d3_visualization(
            node_query=node_query + " UNION " + source_tables_query,
            relationship_query=relationship_query
        )
        
        # Return the HTML
        return HTMLResponse(content=result["html"], status_code=200)
    
    except Exception as e:
        logger.error(f"Error generating Data Vault visualization: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating Data Vault visualization: {str(e)}"
        )


@router.get("/mermaid/{type}", response_class=HTMLResponse)
async def generate_mermaid_diagram(
    type: str = FastAPIPath(..., description="Type of diagram", enum=["lineage", "data-vault", "schema"]),
    object_name: Optional[str] = Query(None, description="Optional object name (table, schema, etc.)"),
    diagram_type: str = Query("flowchart", description="Mermaid diagram type", enum=["flowchart", "classDiagram", "graph"]),
    current_user: User = Depends(get_current_active_user)
):
    """
    Generate a Mermaid diagram
    
    Args:
        type: Type of diagram (lineage, data-vault, schema)
        object_name: Optional object name (table, schema, etc.)
        diagram_type: Mermaid diagram type
        current_user: Current authenticated user
        
    Returns:
        HTML page with Mermaid diagram
    """
    logger.info(f"User {current_user.username} requested Mermaid diagram for {type}")
    
    try:
        # Define queries based on diagram type
        if type == "lineage" and object_name:
            # Table lineage diagram
            node_query = f"""
            MATCH path = (node:Table)-[:SOURCE_OF|CONTAINS*1..5]-(other:Table)
            WHERE node.name = '{object_name}'
            UNWIND nodes(path) AS n
            RETURN DISTINCT n, labels(n) as labels, id(n) as id
            """
            
            relationship_query = f"""
            MATCH path = (node:Table)-[:SOURCE_OF|CONTAINS*1..5]-(other:Table)
            WHERE node.name = '{object_name}'
            UNWIND relationships(path) AS r
            RETURN DISTINCT r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
            """
        
        elif type == "data-vault":
            # Data Vault diagram
            node_query = """
            MATCH (n:DataVaultComponent)
            RETURN n, labels(n) as labels, id(n) as id
            """
            
            relationship_query = """
            MATCH (n:DataVaultComponent)-[r]-(m:DataVaultComponent)
            RETURN r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
            """
        
        elif type == "schema" and object_name:
            # Schema diagram
            node_query = f"""
            MATCH (s:Schema)-[:CONTAINS]->(t:Table)-[:CONTAINS]->(c:Column)
            WHERE s.name = '{object_name}'
            RETURN s as n, labels(s) as labels, id(s) as id
            UNION
            MATCH (s:Schema)-[:CONTAINS]->(t:Table)-[:CONTAINS]->(c:Column)
            WHERE s.name = '{object_name}'
            RETURN t as n, labels(t) as labels, id(t) as id
            UNION
            MATCH (s:Schema)-[:CONTAINS]->(t:Table)-[:CONTAINS]->(c:Column)
            WHERE s.name = '{object_name}'
            RETURN c as n, labels(c) as labels, id(c) as id
            """
            
            relationship_query = f"""
            MATCH (s:Schema)-[r:CONTAINS]->(t:Table)
            WHERE s.name = '{object_name}'
            RETURN r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
            UNION
            MATCH (t:Table)-[r:CONTAINS]->(c:Column)
            WHERE t.schema = '{object_name}'
            RETURN r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
            """
        
        else:
            # Default to all nodes and relationships
            node_query = """
            MATCH (n)
            RETURN n, labels(n) as labels, id(n) as id
            LIMIT 100
            """
            
            relationship_query = """
            MATCH ()-[r]->()
            RETURN r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id
            LIMIT 100
            """
        
        # Generate the Mermaid diagram
        visualizer = GraphVisualizer()
        result = visualizer.generate_mermaid_diagram(
            node_query=node_query,
            relationship_query=relationship_query,
            diagram_type=diagram_type
        )
        
        # Create a simple HTML page with the Mermaid diagram
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Mermaid Diagram</title>
            <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
            <script>
                mermaid.initialize({{ startOnLoad: true }});
            </script>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                }}
                
                .mermaid {{
                    margin: 20px 0;
                }}
                
                .controls {{
                    margin-bottom: 20px;
                }}
            </style>
        </head>
        <body>
            <h1>Knowledge Graph Visualization</h1>
            
            <div class="controls">
                <button onclick="window.history.back()">Back</button>
                <button onclick="window.print()">Print</button>
            </div>
            
            <div class="mermaid">
{result["data"]}
            </div>
        </body>
        </html>
        """
        
        # Return the HTML
        return HTMLResponse(content=html_content, status_code=200)
    
    except Exception as e:
        logger.error(f"Error generating Mermaid diagram: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating Mermaid diagram: {str(e)}"
        )
