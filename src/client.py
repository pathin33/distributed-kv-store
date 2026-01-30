import grpc
import generated.kvstore_pb2 as kvstore_pb2
import generated.kvstore_pb2_grpc as kvstore_pb2_grpc

def run():
    print("Thiết lập kết nối tới Node")
    print("1.Node 1")
    print("2.Node 2")
    print("3.Node 3")
    option_node = int(input("Node để thiết lập kết nối: "))
    #biến lưu địa chị của node thiết lập tới dựa trên option chọn
    target = None 
    match option_node :
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
            print("Lựa chọn phương thức")
            print("1.Put(key,value)")
            print("2.Get(key)")
            print("3.Delete(key)")
            option_service = int(input("Phương thức chon: "))
            #Biến lưu key và value
            Key = None
            Value = None
            match option_service:
                case 1:
                    print("Cần nhập key và value")
                    Key = input("Key: ")
                    Value = input("Value: ")
                    response = stub.Put(kvstore_pb2.PutRequest(key=Key, value=Value,is_replica = False))
                    if response.success:
                        print(f"Kết quả trả về: {response.message}")
                    else:
                        print(f"Kết quả trả về: {response.message}")
                case 2:
                    print("Cần nhập key")
                    Key = input("Key: ")
                    response = stub.Get(kvstore_pb2.GetRequest(key=Key))
                    if response.success:
                        print(f"Kết quả trả về: {response.value}")
                    else:
                        print("Key không tồn tại")
                        print(f"Kết quả trả về: {response.message}")
                case 3:
                    print("Cần nhập key")
                    Key = input("Key: ")
                    response = stub.Delete(kvstore_pb2.DeleteRequest(key=Key))
                    if response.success:
                        print(f"Kết quả trả về: {response.message}")
                    else:
                        print(f"Key không tồn tại")
                        print(f"Kết quả trả về: {response.message}")
                case _:
                    print("Lựa chọn không hợp lệ")
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")


if __name__ == "__main__":
    run()
