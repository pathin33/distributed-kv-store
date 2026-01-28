# distributed-kv-store
# Câu lệnh sinh ra code từ file .proto
```
python -m grpc_tools.protoc \
  -I src/protos \
  --python_out=src/generated \
  --grpc_python_out=src/generated \
  src/protos/kvstore.proto
```
> Hai file kvstore_pb2_grpc.py và kvstore_pb2.py sẽ được sinh ra
