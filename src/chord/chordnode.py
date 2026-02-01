import hashlib
import grpc
import generated.kvstore_pb2 as kvstore_pb2
import generated.kvstore_pb2_grpc as kvstore_pb2_grpc
M = 7 # key sẽ chạy từ 0 đến 2^M - 1 / 0 dến 127
RING_SIZE = 2 ** M # số lượng id trên vòng 128


# hàm hahsh key thành id trên vòng
def get_hash(key):
    h = hashlib.sha1(key.encode()).hexdigest()
    return int(h, 16) % RING_SIZE

class ChordNode:

    def __init__(self, node_id,address,all_nodes_config):
        self.node_id = node_id
        self.address = address
        #lưu trữ thông tin tất cả các node từ config
        self.all_nodes = all_nodes_config
        self.id = get_hash(address)

        self.successor = self
        self.predecessor = self

        self.data = {}       # dữ liệu chính
        self.replica = {}    # bản sao backup
        self.node_stubs = {} #lưu trữ các stub {node_id:stub}
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
