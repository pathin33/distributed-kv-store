# Distributed Key-Value Store

Hệ thống lưu trữ key-value phân tán sử dụng thuật toán **Chord DHT** và giao thức **gRPC**, hỗ trợ sao lưu dữ liệu, phát hiện lỗi và tự động phục hồi.

---

## Mục lục

1. [Yêu cầu hệ thống](#1-yêu-cầu-hệ-thống)
2. [Cài đặt](#2-cài-đặt)
3. [Cấu trúc dự án](#3-cấu-trúc-dự-án)
4. [Sinh code từ file .proto](#4-sinh-code-từ-file-proto)
5. [Khởi chạy hệ thống](#5-khởi-chạy-hệ-thống)
6. [Sử dụng Client](#6-sử-dụng-client)
7. [Kiến trúc hệ thống](#7-kiến-trúc-hệ-thống)
8. [Tính năng nổi bật](#8-tính-năng-nổi-bật)

---

## 1. Yêu cầu hệ thống

- **Python** >= 3.9
- **pip** (trình quản lý gói Python)
- **Git**

---

## 2. Cài đặt

### Bước 1 – Clone repository

```bash
git clone https://github.com/pathin33/distributed-kv-store.git
cd distributed-kv-store
```

### Bước 2 – (Tuỳ chọn) Tạo môi trường ảo

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux / macOS
python3 -m venv venv
source venv/bin/activate
```

### Bước 3 – Cài đặt các gói cần thiết

```bash
pip install grpcio grpcio-tools inquirer
```

| Gói | Mục đích |
|-----|----------|
| `grpcio` | Thư viện runtime gRPC |
| `grpcio-tools` | Công cụ sinh code từ file `.proto` |
| `inquirer` | Giao diện menu tương tác trong terminal |

---

## 3. Cấu trúc dự án

```
distributed-kv-store/
├── config.json              # Cấu hình danh sách node (id, tên, địa chỉ)
├── README.md
└── src/
    ├── node.py              # Điểm vào để khởi động một node gRPC
    ├── client.py            # Client tương tác với hệ thống
    ├── protos/
    │   └── kvstore.proto    # Định nghĩa gRPC service & message
    ├── generated/           # Code được sinh tự động từ .proto
    │   ├── kvstore_pb2.py
    │   └── kvstore_pb2_grpc.py
    ├── chord/
    │   └── chordnode.py     # Logic Chord DHT (routing, replication, failover)
    └── service/
        └── keyvalue.py      # gRPC Servicer – xử lý request từ client/node
```

### Cấu hình node (`config.json`)

```json
{
  "nodes": [
    { "id": 1, "name": "Node1", "address": "127.0.0.1:50051" },
    { "id": 2, "name": "Node2", "address": "127.0.0.1:50052" },
    { "id": 3, "name": "Node3", "address": "127.0.0.1:50053" }
  ]
}
```

> Các node mặc định chạy trên `localhost` với port `50051`, `50052`, `50053`. Bạn có thể thêm/sửa node trong file này.

---

## 4. Sinh code từ file `.proto`

> **Bắt buộc** thực hiện bước này trước khi chạy hệ thống, hoặc khi file `kvstore.proto` có thay đổi.

Chạy lệnh sau từ **thư mục gốc** của dự án:

```bash
# Windows (PowerShell)
python -m grpc_tools.protoc `
  -I src/protos `
  --python_out=src/generated `
  --grpc_python_out=src/generated `
  src/protos/kvstore.proto

# Linux / macOS / Git Bash
python -m grpc_tools.protoc \
  -I src/protos \
  --python_out=src/generated \
  --grpc_python_out=src/generated \
  src/protos/kvstore.proto
```

Sau khi chạy, hai file sau sẽ được tạo ra (hoặc cập nhật) trong `src/generated/`:

- `kvstore_pb2.py` – các class message
- `kvstore_pb2_grpc.py` – stub và servicer gRPC

---

## 5. Khởi chạy hệ thống

### Bước 1 – Mở các terminal riêng biệt cho từng node

Mỗi node cần chạy trong **một terminal riêng**. Chạy từ **thư mục gốc** của dự án:

**Terminal 1 – Node 1:**
```bash
python src/node.py --id 1
```

**Terminal 2 – Node 2:**
```bash
python src/node.py --id 2
```

**Terminal 3 – Node 3:**
```bash
python src/node.py --id 3
```

> Sau khi khởi động, mỗi node sẽ in thông tin khởi động và tự động:
> - Yêu cầu khôi phục dữ liệu từ successor (sau 3 giây)
> - Bắt đầu heartbeat monitor theo dõi các node còn lại (mỗi 5 giây)

**Ví dụ output khi khởi động:**
```
  ========================================================
  [Node 1 | STARTUP  ] Distributed KV Store
  [Node 1 | STARTUP  ] ID      : 1
  [Node 1 | STARTUP  ] Address : 127.0.0.1:50051
  [Node 1 | STARTUP  ] Hash ID : 42
  [Node 1 | STARTUP  ] Nhan Ctrl+C de tat server
  ========================================================
```

### Bước 2 – Dừng node

Nhấn `Ctrl+C` trong terminal của node muốn dừng.

---

## 6. Sử dụng Client

Mở một terminal mới, chạy từ **thư mục gốc**:

```bash
python src/client.py
```

Client sẽ hiển thị menu tương tác dạng mũi tên:

```
========================================
DISTRIBUTED KEY-VALUE STORE
========================================
? Thiết lập kết nối - Sử dụng ↑↓ để chọn, Enter để xác nhận
❯ Node 1
  Node 2
  Node 3
```

Sau khi chọn node, menu chính sẽ xuất hiện:

```
? MENU CHÍNH - Sử dụng ↑↓ để chọn, Enter để xác nhận
❯ Put (key, value) - Thêm/cập nhật dữ liệu
  Get (key) - Lấy giá trị
  Delete (key) - Xóa dữ liệu
  Chuyển Node - Kết nối tới node khác
  Thoát - Đóng chương trình
```

### Các thao tác

| Thao tác | Mô tả |
|----------|-------|
| **Put** | Thêm hoặc cập nhật một cặp key-value |
| **Get** | Lấy giá trị theo key |
| **Delete** | Xóa key khỏi hệ thống |
| **Chuyển Node** | Kết nối lại tới node khác trong cluster |
| **Thoát** | Đóng kết nối và thoát chương trình |

> Client có thể kết nối tới bất kỳ node nào. Node đó sẽ tự động định tuyến request tới đúng node chịu trách nhiệm theo thuật toán Chord.

> **Lưu ý:** Khi chọn node, client tự động **ping** để kiểm tra node còn sống không. Nếu node chết, sẽ thông báo và cho phép chọn lại — không thoát chương trình.

---

## 7. Kiến trúc hệ thống

### Luồng xử lý request (PUT/GET/DELETE)

```
Client gửi yêu cầu
    ↓ (gRPC)
Node nhận request (gRPC Server)
    ↓
KeyValueServicer (xử lý gRPC request)
    ↓
ChordNode.put / get / delete()
    ↓
find_successor(key_id)  ← tính hash SHA-1 của key
    ↓
┌─────────────────────────────────────┐
│        Key thuộc node này?          │
└─────────────────────────────────────┘
         │ CÓ                │ KHÔNG
         ↓                   ↓
    Lưu/Đọc/Xóa        Forward qua gRPC
    từ self.data        → Node chịu trách nhiệm
         │                   │
         └─────────┬──────────┘
                   ↓
             Response trả về Client
```

### Sơ đồ giao tiếp giữa các node (ví dụ PUT)

```
┌─────────┐              ┌─────────┐              ┌─────────┐
│ Client  │              │ Node 1  │              │ Node 2  │
└────┬────┘              └────┬────┘              └────┬────┘
     │                        │                        │
     │  1. PUT("name","Alice") │                        │
     ├───────────────────────>│                        │
     │                        │ 2. find_successor()    │
     │                        │    → Node 2            │
     │                        │ 3. Forward qua gRPC    │
     │                        ├───────────────────────>│
     │                        │                        │ 4. Lưu vào self.data
     │                        │                        │    Replicate → Node 3
     │                        │  5. Response: success  │
     │                        │<───────────────────────┤
     │  6. Response: success  │                        │
     │<───────────────────────┤                        │
```

### Luồng sao lưu dữ liệu (Replication)

```
BƯỚC 1: Client gửi PUT request
  ↓
BƯỚC 2: Node owner nhận và lưu vào self.data (bản chính)
  ↓
BƯỚC 3: Gọi _replicate_to_successor(key, value)
  ↓
BƯỚC 4: Tìm successor node (node kế tiếp trên vòng Chord)
  ↓
BƯỚC 5: Gửi gRPC request với is_replica=True
  ↓
BƯỚC 6: Successor nhận và lưu vào self.replica
```

---

## 8. Tính năng nổi bật

| Tính năng | Mô tả |
|-----------|-------|
| **Chord DHT** | Định tuyến key theo hash SHA-1 trên vòng tròn `2^7 = 128` |
| **Replication** | Mỗi key được sao lưu sang successor node (`self.replica`) |
| **Heartbeat Monitor** | Kiểm tra trạng thái các node mỗi 5 giây |
| **Failover tự động** | Khi node lỗi, replica được promote lên thành primary |
| **Emergency Re-replication** | Khi successor chết, tự động gửi replica sang successor mới để đảm bảo luôn có 2 bản |
| **Replica Cleanup** | Khi node phục hồi, tự động xóa bản sao khẩn cấp và trả quyền sở hữu về đúng node |
| **Data Recovery** | Khi node khởi động lại, tự động yêu cầu khôi phục dữ liệu từ successor |
| **Retry & Forward** | Request được retry tối đa 3 lần, tự bỏ qua node đã lỗi |
| **Client Ping Check** | Ping kiểm tra node trước khi kết nối, thông báo rõ nếu node chết |

