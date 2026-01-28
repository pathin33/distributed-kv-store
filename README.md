# distributed-kv-store
## Câu lệnh sinh ra code từ file .proto
```
python -m grpc_tools.protoc \
  -I src/protos \
  --python_out=src/generated \
  --grpc_python_out=src/generated \
  src/protos/kvstore.proto
```
> Hai file kvstore_pb2_grpc.py và kvstore_pb2.py sẽ được sinh ra
## Cách khởi tạo các node 
```
python src/node.py --id 1
#câu lệnh trên sẽ khởi chạy cho node 1 mở terminal để gõ các node khác --id 2, --id 3
```