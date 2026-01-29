import grpc
from concurrent import futures
import argparse
import json
from service.keyvalue import KeyValueServicer
import generated.kvstore_pb2_grpc as kvstore_pb2_grpc
from chord.chordnode import ChordNode
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
        KeyValueServicer,
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
