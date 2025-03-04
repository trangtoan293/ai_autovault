# Knowledge Graph Module

## Tổng quan

Knowledge Graph module cung cấp khả năng biểu diễn metadata, data lineage, và các thành phần Data Vault dưới dạng đồ thị tri thức (knowledge graph). Module này sử dụng Neo4j để lưu trữ và truy vấn đồ thị, đồng thời tích hợp với LLM để cho phép truy vấn bằng ngôn ngữ tự nhiên.

## Kiến trúc

Module được tổ chức thành các thành phần sau:

- **Models**: Định nghĩa cấu trúc của các node và relationships trong đồ thị
- **Services**: Cung cấp các dịch vụ xây dựng và truy vấn đồ thị
- **API**: Cung cấp các endpoints REST để tương tác với đồ thị

## Các thành phần chính

### Node Types (Loại node)

- **SourceSystem**: Thông tin về hệ thống nguồn dữ liệu
- **Schema**: Schema cơ sở dữ liệu
- **Table**: Bảng trong cơ sở dữ liệu
- **Column**: Cột trong bảng
- **DataVaultComponent**: Thành phần Data Vault (hub, link, satellite)

### Relationship Types (Loại relationship)

- **CONTAINS**: Quan hệ cha-con (SourceSystem → Schema → Table → Column)
- **MAPPED_TO**: Biểu diễn phép biến đổi dữ liệu giữa các cột
- **REFERENCES**: Quan hệ khóa ngoại
- **SOURCE_OF**: Cột nguồn cho thành phần Data Vault
- **DERIVED_FROM**: Được dẫn xuất từ (cho cột có biến đổi)
- **PART_OF**: Quan hệ thuộc về

### Services (Dịch vụ)

- **GraphConnector**: Kết nối đến Neo4j và thực hiện các thao tác cơ bản
- **GraphBuilder**: Xây dựng đồ thị từ metadata và các thành phần Data Vault
- **LineageService**: Truy vấn data lineage
- **SearchService**: Tìm kiếm metadata
- **LLMService**: Tích hợp LLM để truy vấn đồ thị bằng ngôn ngữ tự nhiên
- **GraphVisualizer**: Tạo visualizations từ đồ thị

## Visualizations

Module Knowledge Graph cung cấp các công cụ để tạo visualizations từ dữ liệu trong đồ thị:

### Interactive Visualizations

API cung cấp các endpoints để tạo các interactive visualizations:

```
# Visualize table lineage
GET /api/visualize/lineage/table/{table_name}

# Visualize column lineage
GET /api/visualize/lineage/column/{table_name}/{column_name}

# Visualize Data Vault components
GET /api/visualize/data-vault/{component_type}

# Generate Mermaid diagram
GET /api/visualize/mermaid/{type}?object_name={object_name}
```

### Command-line Tool

Script `generate_visualizations.py` cung cấp các lệnh để tạo visualizations từ command line:

```bash
# Generate table lineage visualization
python scripts/generate_visualizations.py table --table customers

# Generate column lineage visualization
python scripts/generate_visualizations.py column --table customers --column customer_id

# Generate Data Vault component visualization
python scripts/generate_visualizations.py data-vault --type hub

# Generate Mermaid diagram
python scripts/generate_visualizations.py mermaid --type lineage --name customers
```

### Sử dụng GraphVisualizer Service

Nếu cần tích hợp visualization vào code của bạn, bạn có thể sử dụng `GraphVisualizer` service:

```python
from app.knowledge_graph.utils.graph_visualizer import GraphVisualizer

# Create a visualizer
visualizer = GraphVisualizer()

# Generate D3.js visualization
result = visualizer.generate_d3_visualization(
    node_query="MATCH (n:Table) RETURN n, labels(n) as labels, id(n) as id LIMIT 10",
    relationship_query="MATCH (n:Table)-[r]-(m:Table) RETURN r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id LIMIT 20",
    output_file="visualizations/tables.html"
)

# Generate Mermaid diagram
result = visualizer.generate_mermaid_diagram(
    node_query="MATCH (n:Table) RETURN n, labels(n) as labels, id(n) as id LIMIT 10",
    relationship_query="MATCH (n:Table)-[r]-(m:Table) RETURN r, type(r) as type, id(startNode(r)) as source_id, id(endNode(r)) as target_id, id(r) as id LIMIT 20",
    diagram_type="flowchart",
    output_file="visualizations/tables.md"
)
```

## Cài đặt

### Yêu cầu hệ thống

- Neo4j Database (version 5.x)
- Python 3.12+
- Redis (cho caching)

### Cài đặt thư viện

```bash
pip install -r requirements-knowledge-graph.txt
```

### Cấu hình

Cấu hình Neo4j trong file `.env`:

```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j
```

## Sử dụng API

### Xây dựng Knowledge Graph

```bash
curl -X POST "http://localhost:8000/api/knowledge-graph/build" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

### Truy vấn Table Lineage

```bash
curl -X GET "http://localhost:8000/api/knowledge-graph/lineage/table/customers?direction=both" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

### Truy vấn Column Lineage

```bash
curl -X GET "http://localhost:8000/api/knowledge-graph/lineage/column/customers/customer_id?direction=both" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

### Tìm kiếm Metadata

```bash
curl -X GET "http://localhost:8000/api/knowledge-graph/search?query=customer" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

### Truy vấn bằng ngôn ngữ tự nhiên

```bash
curl -X POST "http://localhost:8000/api/knowledge-graph/query" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  -d '{"query":"Hiển thị tất cả các cột là khóa chính"}'
```

## Ví dụ sử dụng

### Xây dựng Knowledge Graph

1. Upload metadata từ file CSV/Excel:
   ```
   POST /api/metadata/upload
   ```

2. Xây dựng knowledge graph từ metadata:
   ```
   POST /api/knowledge-graph/build
   ```

3. Tạo các thành phần Data Vault:
   ```
   POST /api/models/generate
   ```

4. Liên kết các thành phần Data Vault với metadata nguồn:
   ```
   POST /api/knowledge-graph/build-data-vault
   ```

### Truy xuất Data Lineage

1. Xem lineage cấp table:
   ```
   GET /api/knowledge-graph/lineage/table/{table_name}
   ```

2. Xem lineage cấp cột:
   ```
   GET /api/knowledge-graph/lineage/column/{table_name}/{column_name}
   ```

3. Xem lineage của thành phần Data Vault:
   ```
   GET /api/knowledge-graph/lineage/data-vault/{component_name}
   ```

4. Tạo visualization của lineage:
   ```
   GET /api/visualize/lineage/table/{table_name}
   ```

### Tìm kiếm

1. Tìm kiếm theo từ khóa:
   ```
   GET /api/knowledge-graph/search?query={keyword}
   ```

2. Tìm các cột tương tự:
   ```
   GET /api/knowledge-graph/find-similar-columns/{column_name}
   ```

3. Tìm kiếm qua ngôn ngữ tự nhiên:
   ```
   POST /api/knowledge-graph/query
   ```

## Command-Line Tools

Hai command-line tools chính được cung cấp để làm việc với Knowledge Graph:

### 1. manage_knowledge_graph.py

Quản lý cơ bản Knowledge Graph (xây dựng, xóa, kiểm tra status, etc):

```bash
# Clear graph database
python scripts/manage_knowledge_graph.py clear

# Build graph
python scripts/manage_knowledge_graph.py build --source my_source_system

# Show status
python scripts/manage_knowledge_graph.py status
```

### 2. generate_visualizations.py

Tạo visualizations từ Knowledge Graph:

```bash
# Generate table lineage visualization
python scripts/generate_visualizations.py table --table customers

# Generate Mermaid diagram
python scripts/generate_visualizations.py mermaid --type schema --name production
```

## Troubleshooting

### Không thể kết nối đến Neo4j

1. Kiểm tra Neo4j đã chạy:
   ```
   docker ps | grep neo4j
   ```

2. Kiểm tra cấu hình kết nối:
   ```
   NEO4J_URI=bolt://localhost:7687
   NEO4J_USER=neo4j
   NEO4J_PASSWORD=your_password
   ```

3. Kiểm tra logs của Neo4j:
   ```
   docker logs knowledge_graph_neo4j
   ```

### Graph không hiển thị dữ liệu

1. Đảm bảo đã chạy API build:
   ```
   POST /api/knowledge-graph/build
   ```

2. Kiểm tra metadata đã được upload:
   ```
   GET /api/metadata/
   ```

3. Truy cập Neo4j Browser để kiểm tra trực tiếp:
   ```
   http://localhost:7474/browser/
   ```

### Visualization không hiển thị đúng

1. Đảm bảo có dữ liệu trong graph:
   ```
   python scripts/manage_knowledge_graph.py status
   ```

2. Kiểm tra các queries trong visualization:
   ```
   python scripts/manage_knowledge_graph.py execute --script your_query.cypher
   ```

## Truy cập Neo4j Browser

Để truy cập Neo4j Browser và kiểm tra đồ thị trực tiếp:

1. Mở trình duyệt và truy cập:
   ```
   http://localhost:7474/browser/
   ```

2. Đăng nhập với thông tin:
   - Username: neo4j
   - Password: your_password (từ cấu hình)

3. Truy vấn đơn giản để kiểm tra dữ liệu:
   ```cypher
   MATCH (n) RETURN n LIMIT 100
   ```

4. Xem các node theo loại:
   ```cypher
   MATCH (n:Table) RETURN n LIMIT 100
   ```

5. Xem lineage:
   ```cypher
   MATCH path = (source:Table)-[:SOURCE_OF|CONTAINS*1..5]-(target:Table)
   WHERE source.name = 'customers'
   RETURN path
   ```
