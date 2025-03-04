"""
Graph Visualization Utilities.
This module provides utilities to generate visualizations from the Knowledge Graph.
"""
import json
import os
from typing import Dict, Any, List, Optional, Tuple
import base64
from pathlib import Path

from app.knowledge_graph.services.graph_connector import GraphConnector


class GraphVisualizer:
    """Service for generating visualizations from the Knowledge Graph"""
    
    def __init__(self):
        """Initialize the graph visualizer"""
        self.graph = GraphConnector()
    
    def generate_d3_visualization(
        self,
        node_query: str,
        relationship_query: str,
        params: Dict[str, Any] = None,
        output_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate a D3.js visualization from the Knowledge Graph
        
        Args:
            node_query: Cypher query to get nodes
            relationship_query: Cypher query to get relationships
            params: Optional parameters for the queries
            output_file: Optional output file path for the HTML visualization
            
        Returns:
            Dict containing the visualization data and file path
        """
        # Get nodes from the graph
        node_results = self.graph.execute_cypher(node_query, params or {})
        
        # Get relationships from the graph
        rel_results = self.graph.execute_cypher(relationship_query, params or {})
        
        # Process nodes for D3 format
        nodes = []
        node_ids = set()
        
        for record in node_results:
            if "n" in record and "id" in record:
                node = dict(record["n"])
                node_id = record["id"]
                
                # Add node only if not already added
                if node_id not in node_ids:
                    # Extract the first label as group
                    labels = record.get("labels", [])
                    group = labels[0] if labels else "Unknown"
                    
                    # Use name or id as display text
                    name = node.get("name", f"Node {node_id}")
                    
                    # Process node for D3
                    d3_node = {
                        "id": node_id,
                        "name": name,
                        "group": group,
                        "properties": node
                    }
                    
                    nodes.append(d3_node)
                    node_ids.add(node_id)
        
        # Process relationships for D3 format
        links = []
        link_ids = set()
        
        for record in rel_results:
            source_id = record.get("source_id")
            target_id = record.get("target_id")
            rel_type = record.get("type")
            
            if source_id and target_id and source_id in node_ids and target_id in node_ids:
                # Create a unique identifier for the link
                link_id = f"{source_id}_{target_id}_{rel_type}"
                
                # Add link only if not already added
                if link_id not in link_ids:
                    # Process relationship properties
                    properties = {}
                    if "r" in record:
                        properties = dict(record["r"])
                    
                    # Process link for D3
                    d3_link = {
                        "source": source_id,
                        "target": target_id,
                        "type": rel_type,
                        "properties": properties
                    }
                    
                    links.append(d3_link)
                    link_ids.add(link_id)
        
        # Build the visualization data
        visualization_data = {
            "nodes": nodes,
            "links": links
        }
        
        # Generate the visualization HTML
        html = self._generate_d3_html(visualization_data)
        
        # Save to file if output_file is provided
        file_path = None
        if output_file:
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w') as f:
                f.write(html)
            file_path = output_file
        
        return {
            "data": visualization_data,
            "html": html,
            "file_path": file_path
        }
    
    def generate_mermaid_diagram(
        self,
        node_query: str,
        relationship_query: str,
        params: Dict[str, Any] = None,
        output_file: Optional[str] = None,
        diagram_type: str = "flowchart"
    ) -> Dict[str, Any]:
        """
        Generate a Mermaid diagram from the Knowledge Graph
        
        Args:
            node_query: Cypher query to get nodes
            relationship_query: Cypher query to get relationships
            params: Optional parameters for the queries
            output_file: Optional output file path for the diagram
            diagram_type: Type of diagram (flowchart, classDiagram, etc.)
            
        Returns:
            Dict containing the diagram data and file path
        """
        # Get nodes from the graph
        node_results = self.graph.execute_cypher(node_query, params or {})
        
        # Get relationships from the graph
        rel_results = self.graph.execute_cypher(relationship_query, params or {})
        
        # Start building the Mermaid code
        if diagram_type == "flowchart":
            mermaid_code = "flowchart TD\n"
        elif diagram_type == "classDiagram":
            mermaid_code = "classDiagram\n"
        else:
            mermaid_code = f"{diagram_type}\n"
        
        # Process nodes
        node_ids = {}
        
        for record in node_results:
            if "n" in record and "id" in record:
                node = dict(record["n"])
                node_id = record["id"]
                
                # Use name or id as display text
                name = node.get("name", f"Node {node_id}")
                
                # Generate a unique ID for Mermaid
                mermaid_id = f"node_{node_id}"
                node_ids[node_id] = mermaid_id
                
                # Add node to Mermaid code based on diagram type
                if diagram_type == "flowchart":
                    # Extract the first label as shape
                    labels = record.get("labels", [])
                    shape = labels[0] if labels else ""
                    
                    if shape == "Table":
                        mermaid_code += f"    {mermaid_id}[({name})]\n"
                    elif shape == "Column":
                        mermaid_code += f"    {mermaid_id}[/{name}/]\n"
                    elif shape == "DataVaultComponent":
                        mermaid_code += f"    {mermaid_id}{{{{[{name}]}}}} \n"
                    else:
                        mermaid_code += f"    {mermaid_id}[{name}]\n"
                
                elif diagram_type == "classDiagram":
                    # Extract the first label as class
                    labels = record.get("labels", [])
                    class_name = labels[0] if labels else "Unknown"
                    
                    mermaid_code += f"    class {mermaid_id} {{<<{class_name}>>\\n{name}}}\n"
        
        # Process relationships
        for record in rel_results:
            source_id = record.get("source_id")
            target_id = record.get("target_id")
            rel_type = record.get("type")
            
            if source_id and target_id and source_id in node_ids and target_id in node_ids:
                source_mermaid_id = node_ids[source_id]
                target_mermaid_id = node_ids[target_id]
                
                # Add relationship to Mermaid code based on diagram type
                if diagram_type == "flowchart":
                    # Map relationship types to arrow styles
                    arrow = "-->"
                    if rel_type == "CONTAINS":
                        arrow = "--o"
                    elif rel_type == "REFERENCES":
                        arrow = "-.->|references|"
                    elif rel_type == "MAPPED_TO":
                        arrow = "==>|mapped to|"
                    elif rel_type == "SOURCE_OF":
                        arrow = "-->|source of|"
                    
                    mermaid_code += f"    {source_mermaid_id} {arrow} {target_mermaid_id}\n"
                
                elif diagram_type == "classDiagram":
                    # Map relationship types to relationship notation
                    relation = "-->"
                    if rel_type == "CONTAINS":
                        relation = "*-->"
                    elif rel_type == "REFERENCES":
                        relation = "..>"
                    
                    mermaid_code += f"    {source_mermaid_id} {relation} {target_mermaid_id} : {rel_type}\n"
        
        # Save to file if output_file is provided
        file_path = None
        if output_file:
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w') as f:
                f.write(mermaid_code)
            file_path = output_file
        
        return {
            "data": mermaid_code,
            "file_path": file_path
        }
    
    def _generate_d3_html(self, visualization_data: Dict[str, Any]) -> str:
        """
        Generate HTML with D3.js visualization
        
        Args:
            visualization_data: Visualization data
            
        Returns:
            HTML content
        """
        # Convert data to JSON string
        graph_data_json = json.dumps(visualization_data)
        
        # D3.js visualization template
        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Knowledge Graph Visualization</title>
            <style>
                body {{
                    margin: 0;
                    font-family: Arial, sans-serif;
                    overflow: hidden;
                }}
                
                .links line {{
                    stroke: #999;
                    stroke-opacity: 0.6;
                }}
                
                .nodes circle {{
                    stroke: #fff;
                    stroke-width: 1.5px;
                }}
                
                .node-label {{
                    font-size: 10px;
                    pointer-events: none;
                }}
                
                .node-tooltip {{
                    position: absolute;
                    padding: 10px;
                    background-color: rgba(255, 255, 255, 0.9);
                    border: 1px solid #ccc;
                    border-radius: 5px;
                    pointer-events: none;
                    z-index: 10;
                    max-width: 300px;
                    display: none;
                }}
                
                .controls {{
                    position: absolute;
                    top: 10px;
                    left: 10px;
                    background-color: rgba(255, 255, 255, 0.7);
                    padding: 10px;
                    border-radius: 5px;
                }}
                
                .legend {{
                    position: absolute;
                    bottom: 10px;
                    left: 10px;
                    background-color: rgba(255, 255, 255, 0.7);
                    padding: 10px;
                    border-radius: 5px;
                }}
            </style>
            <script src="https://d3js.org/d3.v7.min.js"></script>
        </head>
        <body>
            <div id="tooltip" class="node-tooltip"></div>
            
            <div class="controls">
                <button id="zoom-in">Zoom In</button>
                <button id="zoom-out">Zoom Out</button>
                <button id="reset">Reset</button>
            </div>
            
            <div class="legend" id="legend"></div>
            
            <script>
                // Graph data
                const graphData = {graph_data_json};
                
                // Set up the SVG container
                const width = window.innerWidth;
                const height = window.innerHeight;
                
                const svg = d3.select("body")
                    .append("svg")
                    .attr("width", width)
                    .attr("height", height);
                
                // Create a zoom behavior
                const zoom = d3.zoom()
                    .scaleExtent([0.1, 4])
                    .on("zoom", (event) => {{
                        g.attr("transform", event.transform);
                    }});
                    
                svg.call(zoom);
                
                // Create a container for the graph
                const g = svg.append("g");
                
                // Create a color scale for node groups
                const groups = [...new Set(graphData.nodes.map(node => node.group))];
                const colorScale = d3.scaleOrdinal(d3.schemeCategory10)
                    .domain(groups);
                
                // Create the simulation
                const simulation = d3.forceSimulation(graphData.nodes)
                    .force("link", d3.forceLink(graphData.links)
                        .id(d => d.id)
                        .distance(100))
                    .force("charge", d3.forceManyBody().strength(-300))
                    .force("center", d3.forceCenter(width / 2, height / 2))
                    .force("collide", d3.forceCollide(30));
                
                // Draw the links
                const link = g.append("g")
                    .attr("class", "links")
                    .selectAll("line")
                    .data(graphData.links)
                    .enter().append("line")
                    .attr("stroke-width", d => 2)
                    .attr("stroke", d => {{
                        // Different colors for different relationship types
                        switch(d.type) {{
                            case "CONTAINS": return "#999";
                            case "REFERENCES": return "#0077cc";
                            case "MAPPED_TO": return "#cc0077";
                            case "SOURCE_OF": return "#00cc77";
                            default: return "#999";
                        }}
                    }})
                    .attr("marker-end", d => `url(#arrow-${d.type})`);
                
                // Create arrowhead markers for different relationship types
                const markerTypes = [...new Set(graphData.links.map(link => link.type))];
                
                svg.append("defs").selectAll("marker")
                    .data(markerTypes)
                    .enter().append("marker")
                    .attr("id", d => `arrow-${d}`)
                    .attr("viewBox", "0 -5 10 10")
                    .attr("refX", 15)
                    .attr("refY", 0)
                    .attr("markerWidth", 6)
                    .attr("markerHeight", 6)
                    .attr("orient", "auto")
                    .append("path")
                    .attr("fill", d => {{
                        switch(d) {{
                            case "CONTAINS": return "#999";
                            case "REFERENCES": return "#0077cc";
                            case "MAPPED_TO": return "#cc0077";
                            case "SOURCE_OF": return "#00cc77";
                            default: return "#999";
                        }}
                    }})
                    .attr("d", "M0,-5L10,0L0,5");
                
                // Draw the nodes
                const node = g.append("g")
                    .attr("class", "nodes")
                    .selectAll("circle")
                    .data(graphData.nodes)
                    .enter().append("circle")
                    .attr("r", 10)
                    .attr("fill", d => colorScale(d.group))
                    .call(d3.drag()
                        .on("start", dragstarted)
                        .on("drag", dragged)
                        .on("end", dragended));
                
                // Add labels to nodes
                const label = g.append("g")
                    .attr("class", "node-labels")
                    .selectAll("text")
                    .data(graphData.nodes)
                    .enter().append("text")
                    .attr("class", "node-label")
                    .attr("text-anchor", "middle")
                    .attr("dy", "0.35em")
                    .text(d => d.name)
                    .attr("dx", 0)
                    .attr("dy", -15);
                
                // Add tooltips
                const tooltip = d3.select("#tooltip");
                
                node.on("mouseover", (event, d) => {{
                    tooltip.style("display", "block");
                    
                    // Build tooltip content
                    let tooltipContent = `<strong>${d.name}</strong><br>Type: ${d.group}<br>`;
                    
                    // Add properties
                    tooltipContent += "<hr>";
                    for (const [key, value] of Object.entries(d.properties)) {{
                        tooltipContent += `${key}: ${value}<br>`;
                    }}
                    
                    tooltip.html(tooltipContent)
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY + 10) + "px");
                }})
                .on("mouseout", () => {{
                    tooltip.style("display", "none");
                }})
                .on("mousemove", (event) => {{
                    tooltip
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY + 10) + "px");
                }});
                
                // Update positions during simulation
                simulation.on("tick", () => {{
                    link
                        .attr("x1", d => d.source.x)
                        .attr("y1", d => d.source.y)
                        .attr("x2", d => d.target.x)
                        .attr("y2", d => d.target.y);
                    
                    node
                        .attr("cx", d => d.x)
                        .attr("cy", d => d.y);
                    
                    label
                        .attr("x", d => d.x)
                        .attr("y", d => d.y);
                }});
                
                // Drag functions
                function dragstarted(event, d) {{
                    if (!event.active) simulation.alphaTarget(0.3).restart();
                    d.fx = d.x;
                    d.fy = d.y;
                }}
                
                function dragged(event, d) {{
                    d.fx = event.x;
                    d.fy = event.y;
                }}
                
                function dragended(event, d) {{
                    if (!event.active) simulation.alphaTarget(0);
                    d.fx = null;
                    d.fy = null;
                }}
                
                // Control buttons
                d3.select("#zoom-in").on("click", () => {{
                    svg.transition().call(zoom.scaleBy, 1.2);
                }});
                
                d3.select("#zoom-out").on("click", () => {{
                    svg.transition().call(zoom.scaleBy, 0.8);
                }});
                
                d3.select("#reset").on("click", () => {{
                    svg.transition().call(zoom.transform, d3.zoomIdentity);
                }});
                
                // Create legend
                const legend = d3.select("#legend");
                
                // Node type legend
                legend.append("div")
                    .html("<strong>Node Types</strong>");
                
                groups.forEach(group => {{
                    legend.append("div")
                        .style("margin-top", "5px")
                        .html(`
                            <span style="display: inline-block; width: 12px; height: 12px; background-color: ${colorScale(group)}; margin-right: 5px; border-radius: 50%;"></span>
                            ${group}
                        `);
                }});
                
                // Relationship type legend
                legend.append("div")
                    .style("margin-top", "15px")
                    .html("<strong>Relationship Types</strong>");
                
                markerTypes.forEach(type => {{
                    let color;
                    switch(type) {{
                        case "CONTAINS": color = "#999"; break;
                        case "REFERENCES": color = "#0077cc"; break;
                        case "MAPPED_TO": color = "#cc0077"; break;
                        case "SOURCE_OF": color = "#00cc77"; break;
                        default: color = "#999";
                    }}
                    
                    legend.append("div")
                        .style("margin-top", "5px")
                        .html(`
                            <span style="display: inline-block; width: 20px; height: 3px; background-color: ${color}; margin-right: 5px;"></span>
                            ${type}
                        `);
                }});
            </script>
        </body>
        </html>
        """
        
        return html_template
    
    def close(self):
        """Close the graph connection"""
        self.graph.close()
