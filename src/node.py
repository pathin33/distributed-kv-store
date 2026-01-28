import grpc
from concurrent import futures
import argparse
import json
import generated.kvstore_pb2_grpc as kvstore_pb2_grpc
import generated.kvstore_pb2  as kvstore_pb2

class KeyValueServicer(kvstore_pb2_grpc.KeyValueServiceServicer):
    def __init__(self):
        #Tạo ra 1 dict để lưu trữ các key
        self.db = {}
    
    def Put(self, request, context):
        #khi lưu trữ dữ liệu từ req cho vào db
        self.db[request.key] = request.value
        return kvstore_pb2.PutResponse(success = True,message = "Đã lưu!")
    
    def Get(self, request, context):
        if request.key in self.db :
            value = self.db[request.key]
            return kvstore_pb2.GetResponse(
                success = True ,
                message = "Lấy dữ liệu thành công",    
                value = value
            )
        else:
            return kvstore_pb2.GetResponse(
                success = False,
                message = "Dữ liệu không tồn tại",
                value = ""
            )
    def Delete(self, request, context):
        if request.key in self.db:
            del self.db[request.key]
            return kvstore_pb2.DeleteResponse(
                success = True,
                message = "Xóa thành công"
            )
        else:
            return kvstore_pb2.DeleteResponse(
                success = False,
                message ="Xóa không thành công"
            )

def serve():
    # đọc tham số từ dòng lệnh
    parser = argparse.ArgumentParser(description="Khởi chạy node gRPC")
    parser.add_argument(
        "--id",
        #nhận giá trị sau giá trị --id
        type=int,
        required=True,
        help="ID của node (ví dụ: 1, 2, 3)"
    )
    args = parser.parse_args()#lưu tham số vào biến

    # đọc dữ liệu từ file config
    with open("config.json", "r") as f:
        config = json.load(f)

    # tìm thông tin của node hiện tại
    my_info = None

    for node in config["nodes"]:
        if node["id"] == args.id:
            my_info = node
            break

    if my_info is None:
        raise ValueError(
            f"Không tìm thấy node có id = {args.id} trong config.json"
        )

    my_address = my_info["address"]  # ví dụ: "localhost:50051"

    #Khởi tạo server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )
    #Đăng kí các service với server
    kvstore_pb2_grpc.add_KeyValueServiceServicer_to_server(
        KeyValueServicer(),
        server
    )

    #Lắng nghe cổng kết nối
    server.add_insecure_port(my_address)


    server.start()
    # in ra thông báo của node đang chạy
    print(f"Node {args.id} đang chạy tại {my_address} ...")
    print("Nhấn Ctrl+C để thoát server....")
    try:
        # Giữ cho server chạy
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\nTắt server...")
        server.stop(0)


if __name__ == "__main__":
    serve()
