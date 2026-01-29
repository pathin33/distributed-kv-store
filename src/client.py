import grpc
import generated.kvstore_pb2 as kvstore_pb2
import generated.kvstore_pb2_grpc as kvstore_pb2_grpc

def run():
    print("Thiết lập kết nối tới Node")
    print("1.Node 1")
    print("2.Node 2")
    print("3.Node 3")
    option = int(input("Node để thiết lập kết nối: "))
    #biến lưu địa chị của node thiết lập tới dựa trên option chọn
    target = None 
    match option :
        case 1 :
            print("Node 1 đã được khởi chạy")
            target = "127.0.0.1:50051"
        case 2:
            print("Node 2 đã được khởi chạy")
            target = "127.0.0.1:50052"
        case 3:
            print("Node 3 đã được khởi chạy")
            target = "127.0.0.1:50053"
        case _:
            print("Lựa chọn Node không hợp lệ")
    
    try:
        with grpc.insecure_channel(target) as channel:
            stub = kvstore_pb2_grpc.KeyValueServiceStub(channel)

            print("PUT key = name")
            response = stub.Put(kvstore_pb2.PutRequest(key="name", value="Chord",is_replicac = False))
            print(f"Kết quả trả về: {response}")
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")



if __name__ == "__main__":
    run()
