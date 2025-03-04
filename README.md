## Quản lý Role & Permissions

Ứng dụng hỗ trợ hai role chính:

1. **User**: Có thể upload metadata, tạo models, xem kết quả và thực hiện các tác vụ thông thường
2. **Admin**: Có tất cả quyền của user và thêm quyền triển khai (deploy) models

### Tạo User mới:

```bash
python scripts/create_admin.py --username newuser --password password --email user@example.com
```

*Sửa role thành "user" trong output và thêm vào `app/core/security.py`*

### Quyền hạn theo endpoint:

- Các endpoints thông thường: Yêu cầu role `user`
- Deploy endpoints: Yêu cầu role `admin`# Data Modeling Automation

Một ứng dụng tự động hóa quá trình mô hình hóa dữ liệu (Data Modeling Automation) với sự hỗ trợ của trí tuệ nhân tạo. Ứng dụng này giúp tự động hóa việc tạo mô hình dữ liệu Data Vault, quản lý dự án DBT, và thực hiện các hoạt động liên quan đến metadata.

## Tính năng chính

- **Xử lý Metadata**: Tự động trích xuất metadata từ file CSV/Excel và lưu trữ vào cơ sở dữ liệu
- **Mô hình hóa dữ liệu với AI**: Sử dụng AI để tạo mô hình Data Vault (Hub, Link, Satellite) dựa trên metadata
- **Quản lý dự án DBT**: Khởi tạo, chạy, kiểm thử và triển khai dự án DBT
- **Knowledge Graph**: Biểu diễn metadata và lineage trong graph database (Neo4j) và truy vấn bằng ngôn ngữ tự nhiên
- **Data Lineage**: Theo dõi dòng chảy dữ liệu ở cấp table và column thông qua knowledge graph
- **Tích hợp Git**: Quản lý phiên bản của mô hình dữ liệu và tích hợp với GitLab
- **API RESTful**: API đầy đủ để tích hợp với các hệ thống khác
- **Authentication & Authorization**: Bảo mật API với JWT và role-based access control
- **Caching**: Tích hợp Redis cache để tối ưu performance
- **Background Processing**: Xử lý các tác vụ dài trong background

## Về Data Vault 2.0

Data Vault 2.0 là một phương pháp mô hình hóa dữ liệu được thiết kế cho hệ thống Enterprise Data Warehouse. Ứng dụng này tự động hóa việc tạo ba thành phần chính của Data Vault:

- **Hub**: Chứa các business keys và đại diện cho các thực thể kinh doanh cốt lõi
- **Link**: Thể hiện mối quan hệ giữa các Hubs
- **Satellite**: Lưu trữ các thuộc tính mô tả cho Hubs hoặc Links

Hệ thống sử dụng AI để phân tích metadata và tự động đề xuất cấu trúc Data Vault phù hợp, bao gồm việc xác định business keys, mối quan hệ, và các thuộc tính mô tả.

## Cấu trúc dự án

```
data_modeling_automation/
├── app/                        # Mã nguồn ứng dụng
│   ├── main.py                 # Entry point của FastAPI
│   ├── core/                   # Cấu hình ứng dụng
│   │   ├── config.py           # Cấu hình ứng dụng
│   │   ├── security.py         # Xác thực và phân quyền
│   │   └── logging.py          # Cấu hình logging
│   ├── api/                    # API endpoints
│   │   ├── endpoints/          # Các endpoint routes
│   │   ├── dependencies.py     # API dependencies
│   │   └── error_handlers.py   # Xử lý lỗi API
│   ├── services/               # Business logic
│   │   ├── data_ingestion.py   # Xử lý file CSV/Excel
│   │   ├── metadata_store.py   # Lưu trữ metadata
│   │   ├── model_generator.py  # Tạo model với AI
│   │   ├── dbt_manager.py      # Quản lý dự án DBT
│   │   └── git_manager.py      # Quản lý Git operations
│   ├── models/                 # Data models
│   │   ├── metadata.py         # Metadata models
│   │   ├── config.py           # Configuration models
│   │   └── response.py         # API response models
│   └── utils/                  # Các tiện ích
├── templates/                  # Templates Jinja2
│   ├── dbt/                    # DBT project templates
│   └── models/                 # Data model templates
├── scripts/                    # Utility scripts
│   └── create_admin.py         # Script tạo admin user
├── tests/                      # Unit tests
├── Dockerfile                  # Docker configuration
├── docker-compose.yml          # Docker Compose configuration
├── requirements.txt            # Python dependencies
└── README.md                   # Documentation
```

### Yêu cầu hệ thống

- Python 3.12
- Redis (cho caching)
- PostgreSQL (hoặc SQLite cho development)
- Neo4j (cho Knowledge Graph)
- Git
- DBT (Data Build Tool)

### Cài đặt 

1. Cài đặt Python 3.12 hoặc cao hơn

2. Cài đặt và khởi động Redis:
   ```bash
   # Ubuntu/Debian
   sudo apt install redis-server
   sudo systemctl start redis

   # macOS
   brew install redis
   brew services start redis

   # Windows
   # Tải và cài đặt Redis từ https://github.com/microsoftarchive/redis/releases
   ```

3. Tạo và kích hoạt môi trường ảo:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   .\venv\Scripts\activate   # Windows
   ```

4. Cài đặt các gói phụ thuộc:
   ```bash
   pip install -r requirements.txt
   ```

5. Sao chép file `.env.example` thành `.env` và cập nhật các biến môi trường:
   ```bash
   cp .env.example .env
   ```

6. Tạo tài khoản admin:
   ```bash
   python scripts/create_admin.py --username admin --password yourpassword --email admin@example.com
   ```
   *Theo hướng dẫn cập nhật thông tin user trong `app/core/security.py`*

7. Khởi động ứng dụng:
   ```bash
   uvicorn app.main:app --reload
   ```

8. Truy cập API tại http://localhost:8000

## Sử dụng API

API documentation có sẵn tại http://localhost:8000/docs hoặc http://localhost:8000/redoc sau khi khởi động ứng dụng.

### Authentication

Tất cả API endpoints (ngoại trừ health check và login) yêu cầu JWT authentication.

1. Đăng nhập để lấy access token:
   ```bash
   curl -X POST "http://localhost:8000/api/auth/token" \
     -H "Content-Type: application/json" \
     -d '{"username":"admin","password":"yourpassword"}'
   ```

2. Sử dụng access token trong các requests:
   ```bash
   curl -X GET "http://localhost:8000/api/metadata/" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
   ```

### Endpoint chính:

#### Authentication
- `POST /api/auth/token`: Đăng nhập và lấy access token

#### Metadata
- `POST /api/metadata/upload`: Tải lên file metadata (CSV/Excel)
- `GET /api/metadata/`: Lấy danh sách metadata
- `GET /api/metadata/{id}`: Lấy chi tiết metadata

#### Data Modeling
- `POST /api/models/generate`: Tạo mô hình dữ liệu
- `GET /api/models/templates`: Lấy danh sách templates
- `POST /api/models/batch`: Tạo nhiều mô hình cùng lúc

#### DBT Management
- `POST /api/dbt/init`: Khởi tạo dự án DBT, có thể clone từ GitLab
- `POST /api/dbt/compile`: Compile các mô hình DBT mà không chạy
- `POST /api/dbt/run`: Chạy các mô hình DBT
- `POST /api/dbt/test`: Chạy DBT tests
- `GET /api/dbt/status/{job_id}`: Kiểm tra trạng thái job
- `GET /api/dbt/docs`: Tạo DBT documentation
- `POST /api/dbt/deploy`: Triển khai mô hình (chỉ admin)

#### Knowledge Graph
- `POST /api/knowledge-graph/build`: Xây dựng knowledge graph từ metadata
- `GET /api/knowledge-graph/lineage/table/{table_name}`: Lấy data lineage cấp table
- `GET /api/knowledge-graph/lineage/column/{table_name}/{column_name}`: Lấy data lineage cấp column
- `GET /api/knowledge-graph/search`: Tìm kiếm metadata theo từ khóa
- `POST /api/knowledge-graph/query`: Truy vấn knowledge graph bằng ngôn ngữ tự nhiên

## Knowledge Graph Module

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
- **LLMService**: Tích hợp LLM để truy vấn đồ thị bằng ngôn ngữ tự nhiên


### Ví dụ về quy trình làm việc:

1. Upload metadata:
   ```bash
   curl -X POST "http://localhost:8000/api/metadata/upload" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
     -H "Content-Type: multipart/form-data" \
     -F "file=@path/to/metadata.csv"
   ```

2. Generate Data Vault model:
   ```bash
   curl -X POST "http://localhost:8000/api/models/generate" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"table_name":"customers","model_type":"hub","use_ai_enhancement":true}'
   ```

3. Xây dựng Knowledge Graph:
   ```bash
   curl -X POST "http://localhost:8000/api/knowledge-graph/build" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
   ```

4. Truy vấn Data Lineage:
   ```bash
   curl -X GET "http://localhost:8000/api/knowledge-graph/lineage/table/customers" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
   ```

5. Initialize DBT project:
   ```bash
   curl -X POST "http://localhost:8000/api/dbt/init" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"repo_url":"https://gitlab.com/your-username/your-repo.git"}'
   ```

6. Run DBT models:
   ```bash
   curl -X POST "http://localhost:8000/api/dbt/run" \
     -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"models":["hub_customers","sat_customer_details"]}'
   ```
