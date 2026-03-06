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

NODE_ADDRESSES = {
    1: "127.0.0.1:50051",
    2: "127.0.0.1:50052",
    3: "127.0.0.1:50053",
}

def ping_node(address):
    """Trả về True nếu node tại address đang sống."""
    try:
        channel = grpc.insecure_channel(address)
        stub = kvstore_pb2_grpc.KeyValueServiceStub(channel)
        stub.Ping(kvstore_pb2.PingRequest(), timeout=2)
        return True
    except Exception:
        return False

def connect_to_node():
    choices = ['Node 1', 'Node 2', 'Node 3']

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
    node_id = int(selected.split()[-1])   # "Node 2" → 2
    address = NODE_ADDRESSES[node_id]

    # Kiểm tra node còn sống không trước khi kết nối
    print(f"Đang kiểm tra Node {node_id}...", end=" ", flush=True)
    if ping_node(address):
        print(f"✓ Node {node_id} đang hoạt động.")
        return address, node_id
    else:
        print(f"✗ Node {node_id} không phản hồi (có thể đã chết)!")
        print("Vui lòng chọn node khác hoặc thử lại sau.")
        return None, None

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

    # Lặp cho đến khi kết nối được node còn sống hoặc user chủ động thoát (Ctrl+C)
    target, node_id = None, None
    while target is None:
        target, node_id = connect_to_node()
        if target is None:
            try:
                retry = input("\nThử lại? (Enter = tiếp tục, Ctrl+C = thoát): ")
            except KeyboardInterrupt:
                print("\nTạm biệt!")
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
                    new_target, new_node_id = connect_to_node()
                    if new_target is None:
                        # Node chết hoặc user huỷ → giữ nguyên kết nối cũ
                        print(f"\nGiữ nguyên kết nối với Node {node_id}.")
                    else:
                        channel.close()
                        target, node_id = new_target, new_node_id
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
