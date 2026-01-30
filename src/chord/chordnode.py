import hashlib
M = 7 # key sẽ chạy từ 0 đến 2^M - 1 / 0 dến 127
RING_SIZE = 2 ** M # số lượng id trên vòng 128


# hàm hahsh key thành id trên vòng
def get_hash(key):
    h = hashlib.sha1(key.encode()).hexdigest()
    return int(h, 16) % RING_SIZE

def in_range(x, a, b):
    if a < b:
        return a < x <= b
    return x > a or x <= b


class ChordNode:
    

    def __init__(self, name):
        self.name = name
        self.id = get_hash(name)

        self.successor = self
        self.predecessor = self

        self.data = {}       # dữ liệu chính
        self.replica = {}    # bản sao backup

        ChordNode.nodes.append(self)

    def join(self):
        """Thêm node vào vòng, cập nhật lại successor, predecessor và phân phối lại dữ liệu."""
        ChordNode.rebuild_ring()
        self.redistribute_data_on_join()

    def leave(self):
        """Node rời khỏi vòng, chuyển dữ liệu cho successor, cập nhật lại vòng."""
        # Chuyển dữ liệu chính cho successor
        for k, v in self.data.items():
            self.successor.data[k] = v
        # Chuyển replica cho successor kế tiếp
        for k, v in self.replica.items():
            self.successor.replica[k] = v
        # Xóa node khỏi danh sách
        ChordNode.nodes.remove(self)
        ChordNode.rebuild_ring()
        # Cập nhật lại backup cho các node còn lại
        for node in ChordNode.nodes:
            node.update_backup()

    def redistribute_data_on_join(self):
        """Khi node mới join, các node phải chuyển dữ liệu phù hợp cho node mới."""
        for node in ChordNode.nodes:
            if node == self:
                continue
            move_keys = []
            for k in node.data:
                key_id = get_hash(k)
                owner = self.find_successor(key_id)
                if owner == self:
                    move_keys.append(k)
            for k in move_keys:
                self.data[k] = node.data.pop(k)
        # Sau khi nhận dữ liệu, cập nhật lại backup cho các node
        for node in ChordNode.nodes:
            node.update_backup()

    def update_backup(self):
        """Cập nhật lại replica cho node kế tiếp."""
        self.replica.clear()
        for k, v in self.data.items():
            backup = self.successor
            if backup != self:
                backup.replica[k] = v

    def handle_failure(self):
        """Xử lý khi node này bị hỏng: chuyển dữ liệu và backup cho successor, cập nhật lại vòng."""
        # Chuyển dữ liệu chính cho successor
        for k, v in self.data.items():
            self.successor.data[k] = v
        # Chuyển replica cho successor kế tiếp
        for k, v in self.replica.items():
            self.successor.replica[k] = v
        # Xóa node khỏi danh sách nếu còn tồn tại
        if self in ChordNode.nodes:
            ChordNode.nodes.remove(self)
        ChordNode.rebuild_ring()
        # Cập nhật lại backup cho các node còn lại
        for node in ChordNode.nodes:
            node.update_backup()
   
    # hàm xây dựng thông tin node 
    @staticmethod
    def rebuild_ring():
        nodes = sorted(ChordNode.nodes, key=lambda n: n.id)# sắp xếp các node theo id tăng dần

        for i, node in enumerate(nodes):
            node.successor = nodes[(i + 1) % len(nodes)]#node kế tiếp
            node.predecessor = nodes[(i - 1) % len(nodes)]#node trước đó
    
    # tìm node chịu trách nhiệm key
    def find_successor(self, key_id):
        nodes = sorted(ChordNode.nodes, key=lambda n: n.id)

        for n in nodes:
            if key_id <= n.id:
                #nếu tìm thấy node có id lớn hơn key_id thì đó là node chịu trách nhiệm
                return n

        return nodes[0]  # quay vòng

    # hàm put
    def put(self, key, value):
        key_id = get_hash(key)
        owner = self.find_successor(key_id)

        # lưu bản chính
        owner.data[key] = value

        # replicate sang successor
        backup = owner.successor
        if backup != owner:
            backup.replica[key] = value

        print(f"PUT '{key}' → owner:{owner.name} backup:{backup.name}")

    # hàm get
    def get(self, key):
        key_id = get_hash(key)
        owner = self.find_successor(key_id)

        if key in owner.data:
            return owner.data[key]

        # fallback nếu owner chết đọc replica
        if key in owner.successor.replica:
            return owner.successor.replica[key]

        return None

    # hàm delete
    def delete(self, key):
        key_id = get_hash(key)
        owner = self.find_successor(key_id)

        owner.data.pop(key, None)
        owner.successor.replica.pop(key, None)
