import grpc
import generated.kvstore_pb2 as kvstore_pb2
import generated.kvstore_pb2_grpc as kvstore_pb2_grpc
import inquirer
#thư viện tạo giao diện hỏi đáp ngay trong terminal

def show_menu():
    choices = [
        'Put (key, value) - Thêm/cập nhật dữ liệu',
        'Get (key) - Lấy giá trị',
        'Delete (key) - Xóa dữ liệu',
        'Chuyển Node - Kết nối tới node khác',
        'Thoát - Đóng chương trình'
    ]
    
    questions = [
        inquirer.List('action',
                     message="MENU CHÍNH - Sử dụng ↑↓ để chọn, Enter để xác nhận",
                     choices=choices,
                     carousel=True)
    ]
    
    answers = inquirer.prompt(questions)
    if answers is None:  
        return None
    
    selected = answers['action']
    
    # Map lựa chọn về số tương ứng
    if 'Put' in selected:
        return '1'
    elif 'Get' in selected:
        return '2'
    elif 'Delete' in selected:
        return '3'
    elif 'Chuyển Node' in selected:
        return '4'
    elif 'Thoát' in selected:
        return '0'

def connect_to_node():
    choices = [
        'Node 1',
        'Node 2',
        'Node 3'
    ]
    
    questions = [
        inquirer.List('node',
                     message="Thiết lập kết nối - Sử dụng ↑↓ để chọn, Enter để xác nhận",
                     choices=choices,
                     carousel=True)
    ]
    
    answers = inquirer.prompt(questions)
    if answers is None: 
        return None, None
    
    selected = answers['node']
    
    if 'Node 1' in selected:
        print("Đã kết nối tới Node 1")
        return "127.0.0.1:50051", 1
    elif 'Node 2' in selected:
        print("Đã kết nối tới Node 2")
        return "127.0.0.1:50052", 2
    elif 'Node 3' in selected:
        print("Đã kết nối tới Node 3")
        return "127.0.0.1:50053", 3

def put_operation(stub):
    # Thao tác put
    print("\n--- PUT OPERATION ---")
    key = input("Nhập key: ").strip()
    value = input("Nhập value: ").strip()
    
    if not key or not value:
        print("Key và value không được để trống!")
        return
    
    try:
        response = stub.Put(kvstore_pb2.PutRequest(key=key, value=value, is_replica=False))
        if response.success:
            print(f"{response.message}")
        else:
            print(f"{response.message}")
    except Exception as e:
        print(f"Lỗi: {e}")

def get_operation(stub):
    # Thao tác get
    print("\n--- GET OPERATION ---")
    key = input("Nhập key: ").strip()
    
    if not key:
        print("Key không được để trống!")
        return
    
    try:
        response = stub.Get(kvstore_pb2.GetRequest(key=key))
        if response.success:
            print(f"Value: {response.value}")
        else:
            print(f"{response.message}")
    except Exception as e:
        print(f"Lỗi: {e}")

def delete_operation(stub):
    # Thao tác delete
    print("\n--- DELETE OPERATION ---")
    key = input("Nhập key: ").strip()
    
    if not key:
        print("Key không được để trống!")
        return
    
    try:
        response = stub.Delete(kvstore_pb2.DeleteRequest(key=key))
        if response.success:
            print(f"{response.message}")
        else:
            print(f"{response.message}")
    except Exception as e:
        print(f"Lỗi: {e}")

def run():
    print("="*40)
    print("DISTRIBUTED KEY-VALUE STORE")
    print("="*40)
    
    # Kết nối ban đầu
    target, node_id = connect_to_node()
    
    if target is None:  # User cancelled
        print("\nĐã hủy. Tạm biệt!")
        return
    
    try:
        # Mở kết nối và giữ channel mở trong suốt session
        channel = grpc.insecure_channel(target)
        stub = kvstore_pb2_grpc.KeyValueServiceStub(channel)
        
        while True:
            try:
                print(f"\n[Đang kết nối: Node {node_id}]")
                choice = show_menu()
                
                if choice is None:  # User pressed Ctrl+C
                    print("\n\nNhận Ctrl+C. Đang thoát...")
                    channel.close()
                    print("Tạm biệt!")
                    break
                
                if choice == '1':
                    put_operation(stub)
                elif choice == '2':
                    get_operation(stub)
                elif choice == '3':
                    delete_operation(stub)
                elif choice == '4':
                    # Chuyển node - đóng kết nối cũ và mở kết nối mới
                    channel.close()
                    target, node_id = connect_to_node()
                    if target is None:  # User cancelled
                        print("\nĐã hủy chuyển node.")
                        # Reconnect to previous node
                        channel = grpc.insecure_channel(target)
                        stub = kvstore_pb2_grpc.KeyValueServiceStub(channel)
                    else:
                        channel = grpc.insecure_channel(target)
                        stub = kvstore_pb2_grpc.KeyValueServiceStub(channel)
                elif choice == '0':
                    print("\nĐang đóng kết nối...")
                    channel.close()
                    print("Đã đóng kết nối!")
                    break
                
            except KeyboardInterrupt:
                print("\n\nNhận Ctrl+C. Đang thoát...")
                channel.close()
                print("Tạm biệt!")
                break
            except Exception as e:
                print(f"\nLỗi không mong muốn: {e}")
                
    except Exception as e:
        print(f"Lỗi khi thiết lập kết nối: {e}")


if __name__ == "__main__":
    run()
