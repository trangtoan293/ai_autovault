Bạn muốn tôi hướng dẫn sử dụng repository này cho người mới. Tôi sẽ cung cấp một hướng dẫn từng bước, dễ hiểu cho người mới bắt đầu, để họ có thể cài đặt và sử dụng Data Modeling Automation. Hướng dẫn sẽ bao gồm:

Giới thiệu tổng quan về dự án
Cách cài đặt môi trường (cách đơn giản nhất)
Cách sử dụng cơ bản
Các tác vụ thường gặp
Xử lý sự cố thường gặp

Tôi sẽ sử dụng ngôn ngữ đơn giản, mô tả rõ từng bước, và đảm bảo hướng dẫn phù hợp cho người mới.Hướng Dẫn Sử Dụng Data Modeling Automation cho Người Mới
Chào bạn! Dưới đây là hướng dẫn từng bước để bắt đầu sử dụng Data Modeling Automation. Hướng dẫn này được thiết kế đặc biệt cho người mới, không cần nhiều kiến thức kỹ thuật.
Phần 1: Giới thiệu
Data Modeling Automation là công cụ giúp bạn tự động hóa việc tạo mô hình dữ liệu Data Vault từ metadata. Công cụ này sử dụng AI để phân tích metadata và tạo các mô hình dữ liệu, sau đó quản lý chúng bằng DBT (Data Build Tool).
Những điều bạn có thể làm với công cụ này:

Tải lên file metadata (CSV/Excel)
Tự động tạo mô hình Data Vault
Quản lý và triển khai mô hình dữ liệu
Tích hợp với GitLab để quản lý phiên bản

Phần 2: Cài đặt (Cách đơn giản nhất)
Cách đơn giản nhất để bắt đầu là sử dụng Docker, bạn không cần cài đặt riêng từng thành phần.
Bước 1: Cài đặt các công cụ cần thiết

Cài đặt Docker và Docker Compose:

Tải và cài đặt Docker Desktop từ trang chủ Docker
Docker Compose thường đã được cài đặt kèm Docker Desktop


Cài đặt Git (nếu chưa có):

Tải từ git-scm.com



Bước 2: Clone repository

Mở Terminal hoặc Command Prompt
Chạy lệnh sau để tải repository về máy:
bashCopygit clone https://github.com/yourusername/ai_autovault.git
cd ai_autovault/data_modeling_automation


Bước 3: Cấu hình môi trường

Sao chép file .env.example thành .env:
bashCopy# Windows
copy .env.example .env

# Linux/Mac
cp .env.example .env

Mở file .env bằng trình soạn thảo văn bản (Notepad, VS Code...)
Cập nhật các thông tin cần thiết:

SECRET_KEY: Tạo một chuỗi ngẫu nhiên (có thể là bất kỳ chuỗi nào)
OPENAI_API_KEY: Nhập API key từ OpenAI (nếu có)
Các thông tin kết nối database khác (nếu cần)



Bước 4: Khởi động ứng dụng

Khởi động Docker Desktop (nếu chưa chạy)
Chạy lệnh này để khởi động ứng dụng:
bashCopydocker-compose up -d
Lần đầu tiên có thể mất một lúc để tải về các images
Kiểm tra xem ứng dụng đã chạy chưa:
bashCopydocker-compose ps
Bạn sẽ thấy các services đang chạy

Bước 5: Tạo tài khoản admin

Chạy lệnh sau để tạo tài khoản admin:
bashCopydocker-compose exec app python scripts/create_admin.py --username admin --password yourpassword --email admin@example.com

Script sẽ hiển thị thông tin về user mới và hướng dẫn thêm vào file app/core/security.py
Chạy lệnh sau để mở file security.py:
bashCopy# Windows
notepad app/core/security.py

# Linux/Mac
nano app/core/security.py

Thêm thông tin user vào biến fake_users_db như hướng dẫn từ script

Phần 3: Sử dụng cơ bản
Truy cập API

Mở trình duyệt web
Truy cập địa chỉ: http://localhost:8000/docs
Bạn sẽ thấy trang Swagger UI hiển thị tất cả các API endpoints

Đăng nhập vào hệ thống

Trong Swagger UI, tìm phần /api/auth/token hoặc /api/auth/login
Click vào "Try it out"
Nhập thông tin đăng nhập:
jsonCopy{
  "username": "admin",
  "password": "yourpassword"
}

Click "Execute"
Sao chép access_token từ kết quả trả về
Click vào nút "Authorize" ở đầu trang
Dán token vào ô, thêm tiền tố "Bearer " phía trước
Click "Authorize"

Giờ đây bạn đã đăng nhập và có thể sử dụng tất cả API endpoints.
Phần 4: Các tác vụ thường gặp
Tải lên file metadata

Chuẩn bị file CSV/Excel chứa metadata với các cột bắt buộc:

schema_name
table_name
column_name
column_data_type
table_description
column_description


Trong Swagger UI, tìm endpoint /api/metadata/upload
Click "Try it out"
Click "Browse" để tải file lên
Click "Execute"

Tạo mô hình Data Vault

Tìm endpoint /api/models/generate
Click "Try it out"
Nhập thông tin:
jsonCopy{
  "table_name": "tên_bảng",
  "model_type": "hub",
  "use_ai_enhancement": true
}
(model_type có thể là "hub", "link", hoặc "satellite")
Click "Execute"
Xem kết quả YAML được tạo ra

Khởi tạo dự án DBT

Tìm endpoint /api/dbt/init
Click "Try it out"
Nhập thông tin (để trống nếu không clone từ GitLab):
jsonCopy{
  "repo_url": "https://gitlab.com/your-username/your-repo.git"
}

Click "Execute"

Chạy mô hình DBT

Tìm endpoint /api/dbt/run
Click "Try it out"
Nhập thông tin (các mô hình cần chạy):
jsonCopy{
  "models": ["hub_customers", "sat_customer_details"]
}

Click "Execute"
Lưu ý job_id từ kết quả trả về

Kiểm tra trạng thái job

Tìm endpoint /api/dbt/status/{job_id}
Click "Try it out"
Nhập job_id đã lưu ở bước trước
Click "Execute"
Xem kết quả và trạng thái của job

Phần 5: Xử lý sự cố thường gặp
Không thể kết nối với API

Kiểm tra xem Docker đang chạy không
Chạy docker-compose ps để xem trạng thái các dịch vụ
Kiểm tra logs: docker-compose logs app

Lỗi authentication

Đảm bảo bạn đã tạo user và thêm vào security.py
Kiểm tra xem token đã được thêm đúng định dạng "Bearer [token]"
Token có thể hết hạn sau 30 phút, hãy đăng nhập lại

Lỗi khi tải file metadata

Đảm bảo file CSV/Excel có đủ các cột bắt buộc
Kiểm tra xem file có bị lỗi định dạng không
Đảm bảo dữ liệu không có ký tự đặc biệt gây lỗi

Không có kết quả AI enhancement

Kiểm tra xem bạn đã cung cấp OPENAI_API_KEY trong file .env chưa
Kiểm tra giới hạn API của OpenAI
Thử lại với use_ai_enhancement: false

Phần 6: Tài liệu tham khảo

Trang API Docs: http://localhost:8000/docs
Tài liệu DBT: https://docs.getdbt.com/
Data Vault 2.0: https://www.data-vault.co.uk/what-is-data-vault/

Nếu bạn cần hỗ trợ thêm, hãy tạo issue trên GitHub repository.

Hi vọng hướng dẫn này giúp bạn bắt đầu sử dụng Data Modeling Automation một cách dễ dàng! Hãy bắt đầu bằng việc tải lên file metadata và thử tạo mô hình đầu tiên của bạn.