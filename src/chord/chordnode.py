import hashlib
import grpc
import generated.kvstore_pb2 as kvstore_pb2
import generated.kvstore_pb2_grpc as kvstore_pb2_grpc
import threading
import time
M = 7 # key sẽ chạy từ 0 đến 2^M - 1 / 0 dến 127
RING_SIZE = 2 ** M # số lượng id trên vòng 128
GRPC_TIMEOUT = 2  # Timeout cho mỗi gRPC call (giây)
HEARTBEAT_INTERVAL = 5  # Kiểm tra heartbeat mỗi 5 giây
MAX_RETRY_NODES = 3  # Số node tối đa thử khi forward


# hàm hahsh key thành id trên vòng
def get_hash(key):
    h = hashlib.sha1(key.encode()).hexdigest()
    return int(h, 16) % RING_SIZE

class ChordNode:

    def __init__(self, node_id, address, all_nodes_config):
        self.node_id = node_id
        self.address = address
        # lưu trữ thông tin tất cả các node từ config
        self.all_nodes = all_nodes_config
        self.id = get_hash(address)

        self.successor = self
        self.predecessor = self

        self.data = {}       # dữ liệu chính
        self.replica = {}    # bản sao backup
        self.node_stubs = {}  # lưu trữ các stub {node_id:stub}

        # ADDED FOR NODE FAILURE HANDLING
        self.failed_nodes = set()  # Tập các node_id đã bị hỏng
        self.failed_nodes_lock = threading.Lock()  # Lock để thread-safe access

    def _get_stub(self, node_info):
        
        node_id = node_info["id"]
        
        # Kiểm tra đã có stub chưa (cache để tái sử dụng)
        if node_id in self.node_stubs:
            return self.node_stubs[node_id]
        
        # Tạo stub mới
        address = node_info["address"]
        channel = grpc.insecure_channel(address)
        stub = kvstore_pb2_grpc.KeyValueServiceStub(channel)
        
        # Lưu vào cache
        self.node_stubs[node_id] = stub
        
        return stub

    # tìm node chịu trách nhiệm key
    def find_successor(self, key_id):
        node_ids = []
        for node_info in self.all_nodes:
            #tính id của node bằng việc hash 
            node_hash = get_hash(node_info["address"])
            node_ids.append((node_hash,node_info))
        #sắp xếp theo hash id
        node_ids.sort(key=lambda x: x[0])
        # Tìm node đầu tiên có ID >= key_id
        for node_hash, node_info in node_ids:
            if key_id <= node_hash:
                return node_info  # Trả về thông tin node (id, address)
        return node_ids[0][1]  # quay vòng

    # hàm xử lí put
    def put(self, key, value):
        key_id = get_hash(key)
        owner = self.find_successor(key_id)

        if owner["id"] == self.node_id:
            # Kiểm tra key đã tồn tại chưa
            if key in self.data:
                print(f"Cập nhật {key} trong node hiện tại (value cũ: {self.data[key]} → mới: {value})")
            else:
                print(f"Thêm mới {key} vào node hiện tại")
            self.data[key] = value
        else:
            print(f"Forward PUT {key} sang node {owner['id']}")
            try:
                stub = self._get_stub(owner)
                response = stub.Put(kvstore_pb2.PutRequest(key=key, value=value,is_replica = True))
            except Exception as e:
                print(f"Lỗi khi forward PUT: {e}")
    def get(self, key):
        
        key_id = get_hash(key)
        owner = self.find_successor(key_id)
        
        if owner["id"] == self.node_id:
            # Đọc từ node hiện tại
            value = self.data.get(key, None)
            print(f"Đọc '{key}' từ node {self.node_id}: {value}")
            return value
        else:
            print(f"Forward GET '{key}' từ node {self.node_id} → node {owner['id']}")
            
            try:
                stub = self._get_stub(owner)
                response = stub.Get(kvstore_pb2.GetRequest(key=key))
                
                if response.success:
                    print(f"Forward thành công: {response.value}")
                    return response.value
                else:
                    print(f"Key không tồn tại")
                    return None
            except Exception as e:
                print(f"Lỗi khi forward: {e}")
                return None

    # hàm delete
    def delete(self, key):
        key_id = get_hash(key)
        owner = self.find_successor(key_id)
        if owner["id"] == self.node_id:
            # Kiểm tra key có tồn tại không trước khi xóa
            if key in self.data:
                value = self.data.pop(key)
                print(f"Xóa {key} (value: {value}) khỏi node hiện tại")
                return True
            else:
                print(f"Key '{key}' không tồn tại trong node hiện tại")
                return False
        else:
            print(f"Forward DELETE {key} sang node {owner['id']}")
            try:
                stub = self._get_stub(owner)
                response = stub.Delete(kvstore_pb2.DeleteRequest(key=key))
                print(f"Forward thành công: {response.message}")
                return response.success
            except Exception as e:
                print(f"Lỗi khi forward DELETE: {e}")
                return False

    # ========== ADDED FOR NODE FAILURE HANDLING ==========
    
    def _mark_node_as_failed(self, node_id):
        """
        Đánh dấu một node là đã hỏng.
        
        Giải thích:
        - Thêm node_id vào self.failed_nodes để tránh gọi lại
        - Xóa stub của node này để không tái sử dụng connection lỗi
        - Thread-safe với lock
        """
        with self.failed_nodes_lock:
            if node_id not in self.failed_nodes:
                self.failed_nodes.add(node_id)
                print(f" Node {node_id} đã được đánh dấu là FAILED")
                self._invalidate_stub(node_id)
    
    def _invalidate_stub(self, node_id):
        """
        Xóa stub của node bị hỏng khỏi cache.
        
        Giải thích:
        - Loại bỏ cached stub để tránh tái sử dụng connection lỗi
        - Lần gọi tiếp theo sẽ tạo stub mới (hoặc skip nếu node vẫn failed)
        """
        if node_id in self.node_stubs:
            del self.node_stubs[node_id]
            print(f" Đã xóa stub của node {node_id}")
    
    def _is_node_alive(self, node_info):
        """
        Kiểm tra xem node có còn sống không.
        
        Giải thích:
        - Nếu node đã trong failed_nodes → return False ngay
        - Thử ping node bằng gRPC call đơn giản với timeout ngắn
        - Nếu exception/timeout → đánh dấu failed và return False
        
        Args:
            node_info: Dict chứa {"id", "address"}
        
        Returns:
            True nếu node còn sống, False nếu hỏng
        """
        node_id = node_info["id"]
        
        # Kiểm tra cache failed nodes
        with self.failed_nodes_lock:
            if node_id in self.failed_nodes:
                return False
        
        # Không ping chính node này
        if node_id == self.node_id:
            return True
        
        try:
            stub = self._get_stub(node_info)
            # Thử Get một key không tồn tại để check kết nối
            # Timeout ngắn để phát hiện nhanh
            response = stub.Get(
                kvstore_pb2.GetRequest(key="__health_check__"),
                timeout=GRPC_TIMEOUT
            )
            return True
        except Exception as e:
            print(f"Node {node_id} không phản hồi: {e}")
            self._mark_node_as_failed(node_id)
            return False
    
    def find_successor_with_fallback(self, key_id, exclude_nodes=None):
        """
        Tìm node chịu trách nhiệm key_id, bỏ qua các node bị hỏng.
        
        Giải thích:
        - Lấy danh sách tất cả node, sort theo hash ID
        - Lọc bỏ các node trong failed_nodes và exclude_nodes
        - Tìm node đầu tiên có ID >= key_id (giống find_successor gốc)
        - Nếu không tìm thấy → quay vòng lấy node đầu tiên
        
        Args:
            key_id: Hash ID của key
            exclude_nodes: Set các node_id cần bỏ qua (tránh vòng lặp)
        
        Returns:
            node_info của successor, hoặc None nếu không còn node sống
        """
        if exclude_nodes is None:
            exclude_nodes = set()
        
        node_ids = []
        for node_info in self.all_nodes:
            node_id = node_info["id"]
            node_hash = get_hash(node_info["address"])
            
            # Bỏ qua node bị hỏng hoặc trong exclude list
            with self.failed_nodes_lock:
                if node_id in self.failed_nodes or node_id in exclude_nodes:
                    continue
            
            node_ids.append((node_hash, node_info))
        
        if not node_ids:
            print("KHÔNG CÒN NODE SỐNG NÀO!")
            return None
        
        # Sắp xếp theo hash ID
        node_ids.sort(key=lambda x: x[0])
        
        # Tìm successor
        for node_hash, node_info in node_ids:
            if key_id <= node_hash:
                return node_info
        
        # Quay vòng
        return node_ids[0][1]
    
    def _promote_replica_to_primary(self):
        """
        Promote dữ liệu replica thành dữ liệu chính.
        
        Giải thích:
        - Khi phát hiện node owner bị hỏng, node kế tiếp có thể có replica
        - Di chuyển tất cả replica → data chính
        - Xóa replica sau khi promote
        - Được gọi khi node nhận trách nhiệm cho key mới do node trước bị hỏng
        """
        if self.replica:
            print(f"Promoting {len(self.replica)} replica keys thành primary data")
            for key, value in self.replica.items():
                if key not in self.data:
                    self.data[key] = value
                    print(f" {key} = {value}")
            self.replica.clear()
    
    def _replicate_to_successor(self, key, value):
        """
        Tạo bản sao cho successor node (để fault tolerance).
        
        Giải thích:
        - Tìm successor node của node hiện tại
        - Gửi data sang successor để lưu vào replica
        - Nếu successor bị hỏng → bỏ qua (sẽ tìm successor khác)
        
        Args:
            key: Key cần replicate
            value: Value tương ứng
        """
        # Tìm successor của node hiện tại (không phải successor của key)
        my_hash = self.id
        successor_node = self.find_successor_with_fallback(my_hash + 1)
        
        if successor_node and successor_node["id"] != self.node_id:
            try:
                stub = self._get_stub(successor_node)
                # Gửi với flag is_replica=True
                response = stub.Put(
                    kvstore_pb2.PutRequest(key=key, value=value, is_replica=True),
                    timeout=GRPC_TIMEOUT
                )
                print(f" Replicated {key} sang node {successor_node['id']}")
            except Exception as e:
                print(f"Không thể replicate sang successor: {e}")
    
    def _forward_with_retry(self, operation, key, value=None, max_retries=MAX_RETRY_NODES):
        """
        Forward operation (PUT/GET/DELETE) với retry logic.
        
        Giải thích:
        - Tìm owner node cho key
        - Thử gọi operation
        - Nếu thất bại → đánh dấu node failed, tìm node kế tiếp
        - Lặp lại tối đa max_retries lần
        - Tránh vòng lặp vô hạn bằng exclude_nodes
        
        Args:
            operation: "PUT", "GET", hoặc "DELETE"
            key: Key cần thao tác
            value: Value (chỉ dùng cho PUT)
            max_retries: Số lần thử tối đa
        
        Returns:
            Response từ gRPC call, hoặc None nếu thất bại
        """
        key_id = get_hash(key)
        exclude_nodes = set()
        
        for attempt in range(max_retries):
            # Tìm owner node, bỏ qua các node đã thử
            owner = self.find_successor_with_fallback(key_id, exclude_nodes)
            
            if owner is None:
                print(f"Không còn node nào để xử lý {operation} cho key '{key}'")
                return None
            
            # Nếu owner là chính node này, xử lý local
            if owner["id"] == self.node_id:
                if operation == "PUT":
                    self.data[key] = value
                    print(f"Lưu {key} vào node hiện tại (sau fallback)")
                    self._replicate_to_successor(key, value)
                    return True
                elif operation == "GET":
                    result = self.data.get(key, None)
                    print(f"Đọc {key} từ node hiện tại (sau fallback): {result}")
                    return result
                elif operation == "DELETE":
                    self.data.pop(key, None)
                    print(f"Xóa {key} từ node hiện tại (sau fallback)")
                    return True
            
            # Forward sang owner node
            print(f"🔄 Attempt {attempt+1}/{max_retries}: Forward {operation} '{key}' → node {owner['id']}")
            
            try:
                stub = self._get_stub(owner)
                
                if operation == "PUT":
                    response = stub.Put(
                        kvstore_pb2.PutRequest(key=key, value=value, is_replica=False),
                        timeout=GRPC_TIMEOUT
                    )
                    print(f"PUT thành công qua node {owner['id']}")
                    return response
                
                elif operation == "GET":
                    response = stub.Get(
                        kvstore_pb2.GetRequest(key=key),
                        timeout=GRPC_TIMEOUT
                    )
                    if response.success:
                        print(f"GET thành công qua node {owner['id']}: {response.value}")
                        return response.value
                    else:
                        print(f" Key '{key}' không tồn tại")
                        return None
                
                elif operation == "DELETE":
                    response = stub.Delete(
                        kvstore_pb2.DeleteRequest(key=key),
                        timeout=GRPC_TIMEOUT
                    )
                    print(f"DELETE thành công qua node {owner['id']}")
                    return response
            
            except Exception as e:
                print(f"Lỗi khi forward {operation} qua node {owner['id']}: {e}")
                self._mark_node_as_failed(owner["id"])
                exclude_nodes.add(owner["id"])
                # Tiếp tục với retry
        
        print(f"Đã thử {max_retries} lần nhưng thất bại cho {operation} '{key}'")
        return None
    
    def start_heartbeat_monitor(self):
        """
        Khởi động thread nền để monitor health của các node.
        
        Giải thích:
        - Chạy trong background thread
        - Mỗi HEARTBEAT_INTERVAL giây, ping tất cả các node
        - Phát hiện node recover: xóa khỏi failed_nodes
        - Phát hiện node mới failed: đánh dấu vào failed_nodes
        - Promote replica nếu cần thiết
        """
        def monitor():
            while True:
                time.sleep(HEARTBEAT_INTERVAL)
                print(f"\n  Heartbeat check từ node {self.node_id}")
                
                for node_info in self.all_nodes:
                    node_id = node_info["id"]
                    
                    # Bỏ qua chính node này
                    if node_id == self.node_id:
                        continue
                    
                    is_alive = self._is_node_alive(node_info)
                    
                    # Phát hiện node recover
                    with self.failed_nodes_lock:
                        was_failed = node_id in self.failed_nodes
                    
                    if was_failed and is_alive:
                        with self.failed_nodes_lock:
                            self.failed_nodes.discard(node_id)
                        print(f"Node {node_id} đã RECOVERY!")
                    
                # Promote replica nếu có node failed mà ta đang giữ backup của nó
                if self.replica:
                    self._promote_replica_to_primary()
        
        heartbeat_thread = threading.Thread(target=monitor, daemon=True)
        heartbeat_thread.start()
        print(f"Heartbeat monitor started cho node {self.node_id}")

