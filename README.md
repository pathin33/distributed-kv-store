# distributed-kv-store
## Câu lệnh sinh ra code từ file .proto
```
python -m grpc_tools.protoc `
  -I src/protos `
  --python_out=src/generated `
  --grpc_python_out=src/generated `
  src/protos/kvstore.proto
```
> Hai file kvstore_pb2_grpc.py và kvstore_pb2.py sẽ được sinh ra
## Cách khởi tạo các node 
```
python src/node.py --id 1
#câu lệnh trên sẽ khởi chạy cho node 1 mở terminal để gõ các node khác --id 2, --id 3
```
## Cách khởi tạo client
```
python src/client.py
```
## Luồng hoạt động cơ bản của hệ thống
```
Client gửi yêu cầu 
    ↓ (gRPC)
Node (gRPC Server nhận request)
    ↓
KeyValueServicer (Xử lý gRPC request: Put/Get/Delete)
    ↓
ChordNode.put/get/delete() (Xử lý logic Chord)
    ↓
find_successor(key_id) (Tìm node chịu trách nhiệm)
    ↓
┌─────────────────────────────────────┐
│ Key thuộc node hiện tại?            │
└─────────────────────────────────────┘
         │                    │
         │ CÓ                 │ KHÔNG
         ↓                    ↓
    Lưu/Đọc/Xóa         Forward qua gRPC
    từ self.data         sang node khác
         │                    │
         │                    ↓
         │              Node khác nhận
         │                    │
         │              KeyValueServicer
         │                    │
         │              ChordNode.put/get/delete()
         │                    │
         │              Lưu/Đọc/Xóa từ self.data
         │                    │
         └────────┬───────────┘
                  ↓
            Response trả về
                  ↓
            KeyValueServicer
                  ↓ (gRPC)
              Client
```

## Sơ đồ biểu thị việc giao tiếp giữa các node
```
┌─────────┐                    ┌─────────┐                    ┌─────────┐
│ Client  │                    │ Node 1  │                    │ Node 2  │
└────┬────┘                    └────┬────┘                    └────┬────┘
     │                              │                              │
     │ 1. PUT("name", "Alice")      │                              │
     ├─────────────────────────────>│                              │
     │                              │                              │
     │                              │ 2. find_successor("name")    │
     │                              │    → Node 2                  │
     │                              │                              │
     │                              │ 3. stub.Put() qua gRPC       │
     │                              ├─────────────────────────────>│
     │                              │                              │
     │                              │                              │ 4. Lưu vào
     │                              │                              │    self.data
     │                              │                              │
     │                              │ 5. Response: success         │
     │                              │<─────────────────────────────┤
     │                              │                              │
     │ 6. Response: success         │                              │
     │<─────────────────────────────┤                              │
     │                              │                              │
     
```