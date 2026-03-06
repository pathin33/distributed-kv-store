import hashlib
import grpc
import sys
sys.stdout.reconfigure(line_buffering=True)
import generated.kvstore_pb2 as kvstore_pb2
import generated.kvstore_pb2_grpc as kvstore_pb2_grpc
import threading
import time

M = 7                    # Số bit hash → vòng Chord có 2^7 = 128 vị trí
RING_SIZE = 2 ** M
GRPC_TIMEOUT = 2         # Timeout mỗi lần gọi gRPC (giây)
HEARTBEAT_INTERVAL = 5   # Chu kỳ kiểm tra heartbeat (giây)
MAX_RETRY_NODES = 3      # Số lần thử lại tối đa khi forward thất bại


def get_hash(key):
    """Tính hash SHA-1 của key rồi ánh xạ vào vòng Chord [0, 127]."""
    h = hashlib.sha1(key.encode()).hexdigest()
    return int(h, 16) % RING_SIZE


def _tag(node_id, category):
    """Tạo prefix log chuẩn: [Node X | CATEGORY ]"""
    return f"[Node {node_id} | {category:<9}]"


class ChordNode:

    def __init__(self, node_id, address, all_nodes_config):
        self.node_id = node_id
        self.address = address
        self.all_nodes = all_nodes_config
        self.id = get_hash(address)   # Vị trí của node trên vòng Chord

        self.successor = self
        self.predecessor = self

        self.data = {}       # Dữ liệu chính node này sở hữu {key: value}
        self.replica = {}    # Bản sao từ predecessor {key: (value, owner_id)}
        self.node_stubs = {} # Cache gRPC stub {node_id: stub}

        self.failed_nodes = set()
        self.failed_nodes_lock = threading.Lock()


    def _get_stub(self, node_info):
        """Trả về gRPC stub của node, ưu tiên dùng cache."""
        node_id = node_info["id"]
        if node_id in self.node_stubs:
            return self.node_stubs[node_id]
        channel = grpc.insecure_channel(node_info["address"])
        stub = kvstore_pb2_grpc.KeyValueServiceStub(channel)
        self.node_stubs[node_id] = stub
        return stub

    def _log(self, category, msg):
        """In log có định dạng [Node X | CATEGORY] msg."""
        print(f"{_tag(self.node_id, category)} {msg}")

    def _print_storage_status(self):
        """In trạng thái data và replica hiện tại của node."""
        replica_display = {k: v for k, (v, _) in self.replica.items()}
        sep = "-" * 56
        print(f"\n  {sep}")
        print(f"  {_tag(self.node_id, 'STORAGE')} Trang thai hien tai")
        print(f"  {sep}")
        print(f"  {'Data (chinh):':<18} {self.data if self.data else '(trong)'}")
        print(f"  {'Replica (sao):':<18} {replica_display if replica_display else '(trong)'}")
        print(f"  {sep}\n")

    #Định tuyến Chord

    def find_successor(self, key_id):
        """Tìm node chịu trách nhiệm cho key_id (không lọc node lỗi)."""
        node_ids = sorted(
            [(get_hash(n["address"]), n) for n in self.all_nodes],
            key=lambda x: x[0]
        )
        for node_hash, node_info in node_ids:
            if key_id <= node_hash:
                return node_info
        return node_ids[0][1]  # Quay vòng
    #Bỏ qua node khi node lỗi
    def find_successor_with_fallback(self, key_id, exclude_nodes=None):
        """Tìm successor, bỏ qua các node đã lỗi hoặc trong exclude_nodes."""
        if exclude_nodes is None:
            exclude_nodes = set()

        node_ids = []
        for node_info in self.all_nodes:
            nid = node_info["id"]
            with self.failed_nodes_lock:
                if nid in self.failed_nodes or nid in exclude_nodes:
                    continue
            node_ids.append((get_hash(node_info["address"]), node_info))

        if not node_ids:
            self._log("FAILOVER", "!!! KHONG CON NODE SONG NAO TRONG CLUSTER !!!")
            return None

        node_ids.sort(key=lambda x: x[0])
        for node_hash, node_info in node_ids:
            if key_id <= node_hash:
                return node_info
        return node_ids[0][1]

    #Các thao tác CRUD

    def put(self, key, value):
        """Lưu key-value: xử lý local nếu là owner, ngược lại forward."""
        key_id = get_hash(key)
        owner = self.find_successor(key_id)

        if owner["id"] == self.node_id:
            if key in self.data:
                self._log("PUT", f"UPDATE '{key}': '{self.data[key]}' --> '{value}'")
            else:
                self._log("PUT", f"INSERT '{key}' = '{value}'")
            self.data[key] = value
            self._replicate_to_successor(key, value)
            self._print_storage_status()
        else:
            self._log("PUT", f"FORWARD '{key}' --> Node {owner['id']}")
            self._forward_with_retry("PUT", key, value)

    def get(self, key):
        """Lấy giá trị của key: đọc local nếu là owner, ngược lại forward."""
        key_id = get_hash(key)
        owner = self.find_successor(key_id)

        if owner["id"] == self.node_id:
            value = self.data.get(key, None)
            if value is not None:
                self._log("GET", f"HIT  '{key}' = '{value}'")
            else:
                self._log("GET", f"MISS '{key}' (khong ton tai)")
            self._print_storage_status()
            return value
        else:
            self._log("GET", f"FORWARD '{key}' --> Node {owner['id']}")
            return self._forward_with_retry("GET", key)

    def delete(self, key):
        """Xóa key: xóa local + replica nếu là owner, ngược lại forward."""
        key_id = get_hash(key)
        owner = self.find_successor(key_id)

        if owner["id"] == self.node_id:
            if key in self.data:
                value = self.data.pop(key)
                self._log("DELETE", f"OK  '{key}' (value: '{value}')")
                self._delete_replica_from_successor(key)
                self._print_storage_status()
                return True
            else:
                self._log("DELETE", f"MISS '{key}' (khong ton tai)")
                return False
        else:
            self._log("DELETE", f"FORWARD '{key}' --> Node {owner['id']}")
            result = self._forward_with_retry("DELETE", key)
            return result is not None

    # Phát hiện node lỗi

    def _mark_node_as_failed(self, node_id):
        """Thêm node_id vào failed_nodes và xóa stub cache của nó."""
        with self.failed_nodes_lock:
            if node_id not in self.failed_nodes:
                self.failed_nodes.add(node_id)
                self._log("FAILOVER", f"Node {node_id} duoc danh dau FAILED")
                self._invalidate_stub(node_id)

    def _invalidate_stub(self, node_id):
        """Xóa stub cache của node để buộc tạo kết nối mới lần sau."""
        if node_id in self.node_stubs:
            del self.node_stubs[node_id]
            self._log("FAILOVER", f"Da xoa stub cache cua Node {node_id}")

    def _is_node_alive(self, node_info):
        """Ping gRPC để kiểm tra node còn sống không; cập nhật failed_nodes."""
        node_id = node_info["id"]
        if node_id == self.node_id:
            return True
        try:
            stub = self._get_stub(node_info)
            stub.Ping(kvstore_pb2.PingRequest(), timeout=GRPC_TIMEOUT)
            with self.failed_nodes_lock:
                if node_id in self.failed_nodes:
                    self.failed_nodes.discard(node_id)
                    self._invalidate_stub(node_id)
            return True
        except Exception:
            self._mark_node_as_failed(node_id)
            return False
   #Gửi dữ liệu đến node tiếp theo để sao lưu
    def _replicate_to_successor(self, key, value):
        """Gửi bản sao (is_replica=True) của key sang successor."""
        successor_node = self.find_successor_with_fallback(self.id + 1)
        if successor_node and successor_node["id"] != self.node_id:
            try:
                stub = self._get_stub(successor_node)
                stub.Put(
                    kvstore_pb2.PutRequest(
                        key=key, value=value,
                        is_replica=True, owner_node_id=self.node_id
                    ),
                    timeout=GRPC_TIMEOUT
                )
                self._log("REPLICA", f"Sao chep '{key}' --> Node {successor_node['id']}")
            except Exception as e:
                self._log("REPLICA", f"THAT BAI khi sao chep '{key}': {e}")
    #Tìm dữ liệu bản sao và xóa nó
    def _delete_replica_from_successor(self, key):
        """Yêu cầu successor xóa bản sao của key."""
        successor_node = self.find_successor_with_fallback(self.id + 1)
        if successor_node and successor_node["id"] != self.node_id:
            try:
                stub = self._get_stub(successor_node)
                stub.Delete(
                    kvstore_pb2.DeleteRequest(key=key, is_replica=True),
                    timeout=GRPC_TIMEOUT
                )
                self._log("REPLICA", f"Xoa replica '{key}' tren Node {successor_node['id']}")
            except Exception as e:
                self._log("REPLICA", f"THAT BAI khi xoa replica '{key}': {e}")

    #Gửi lại toàn bộ data chính dưới dạng replica tới node vừa phục hồi
    def _re_replicate_all_to(self, node_info):
        """Gửi lại toàn bộ data chính dưới dạng replica tới node vừa phục hồi."""
        if not self.data:
            return
        target_id = node_info["id"]
        self._log("REPLICA", f"Re-replicate {len(self.data)} key --> Node {target_id} (vua phuc hoi)")
        try:
            stub = self._get_stub(node_info)
            for key, value in self.data.items():
                stub.Put(
                    kvstore_pb2.PutRequest(
                        key=key, value=value,
                        is_replica=True, owner_node_id=self.node_id
                    ),
                    timeout=GRPC_TIMEOUT
                )
                self._log("REPLICA", f"  Re-send '{key}' --> Node {target_id}")
        except Exception as e:
            self._log("REPLICA", f"THAT BAI khi re-replicate --> Node {target_id}: {e}")

    #Promote / Demote
    #Khi node bị lỗi, node tiếp theo sẽ nhận dữ liệu bản sao của node lỗi thành dữ liệu chính
    def _promote_replica_to_primary(self):
        """Chuyển replica của node đã chết thành data chính để tiếp tục phục vụ."""
        if not self.replica:
            return

        with self.failed_nodes_lock:
            failed = set(self.failed_nodes)

        promoted = []
        for key, (value, owner_id) in list(self.replica.items()):
            if owner_id in failed and key not in self.data:
                self.data[key] = value
                promoted.append(key)
                self._log("FAILOVER", f"PROMOTE replica '{key}' = '{value}' (owner Node {owner_id} da chet)")

        for key in promoted:
            del self.replica[key]

        if promoted:
            self._log("FAILOVER", f"Da promote {len(promoted)} key thanh primary data")
            self._print_storage_status()

    def _demote_promoted_data(self, node_info):
        """Trả lại quyền sở hữu data về node gốc vừa phục hồi (promoted → replica)."""
        recovered_id = node_info["id"]
        demoted = []

        for key, value in list(self.data.items()):
            owner = self.find_successor(get_hash(key))
            if owner["id"] == recovered_id:
                self.replica[key] = (value, recovered_id)
                del self.data[key]
                demoted.append(key)
                self._log("FAILOVER", f"DEMOTE '{key}' tu primary -> replica (Node {recovered_id} da song lai)")

        if demoted:
            self._log("FAILOVER", f"Da demote {len(demoted)} key, tra lai chu so huu Node {recovered_id}")
            self._print_storage_status()

    #Forward với retry

    def _forward_with_retry(self, operation, key, value=None, max_retries=MAX_RETRY_NODES):
        """
        Chuyển tiếp PUT/GET/DELETE đến đúng node, tự retry nếu node lỗi.
        Nếu tất cả node đều lỗi, xử lý fallback ngay tại node hiện tại.
        """
        key_id = get_hash(key)
        exclude_nodes = set()

        for attempt in range(max_retries):
            owner = self.find_successor_with_fallback(key_id, exclude_nodes)

            if owner is None:
                self._log(operation, f"THAT BAI ({attempt+1} lan): khong con node nao xu ly '{key}'")
                return None

            # Fallback: chính node này là owner sau khi bỏ hết node lỗi
            if owner["id"] == self.node_id:
                if operation == "PUT":
                    self._log("PUT", f"FALLBACK luu '{key}' tai chinh node nay")
                    self.data[key] = value
                    self._replicate_to_successor(key, value)
                    self._print_storage_status()
                    return True
                elif operation == "GET":
                    result = self.data.get(key, None)
                    self._log("GET", f"FALLBACK: '{key}' = '{result}'")
                    return result
                elif operation == "DELETE":
                    self._log("DELETE", f"FALLBACK xoa '{key}' tai chinh node nay")
                    self.data.pop(key, None)
                    self._print_storage_status()
                    return True

            self._log(operation, f"Thu {attempt+1}/{max_retries}: '{key}' --> Node {owner['id']}")

            try:
                stub = self._get_stub(owner)

                if operation == "PUT":
                    response = stub.Put(
                        kvstore_pb2.PutRequest(key=key, value=value, is_replica=False),
                        timeout=GRPC_TIMEOUT
                    )
                    self._log("PUT", f"OK  '{key}' qua Node {owner['id']}")
                    return response

                elif operation == "GET":
                    response = stub.Get(
                        kvstore_pb2.GetRequest(key=key),
                        timeout=GRPC_TIMEOUT
                    )
                    if response.success:
                        self._log("GET", f"OK  '{key}' = '{response.value}' (tu Node {owner['id']})")
                        return response.value
                    else:
                        self._log("GET", f"MISS '{key}' (khong ton tai tren Node {owner['id']})")
                        return None

                elif operation == "DELETE":
                    response = stub.Delete(
                        kvstore_pb2.DeleteRequest(key=key),
                        timeout=GRPC_TIMEOUT
                    )
                    self._log("DELETE", f"OK  '{key}' qua Node {owner['id']}")
                    return response

            except Exception as e:
                # Node không phản hồi → đánh dấu lỗi, thử node tiếp theo
                self._log(operation, f"LOI khi goi Node {owner['id']}: node co the da chet")
                self._mark_node_as_failed(owner["id"])
                exclude_nodes.add(owner["id"])

        self._log(operation, f"THAT BAI sau {max_retries} lan thu cho '{key}'")
        return None

    #Phục hồi dữ liệu yêu cầu tới node tiếp theo

    def recover_data_from_successor(self):
        """Lấy lại dữ liệu từ successor khi node vừa khởi động lại sau sự cố."""
        sep = "=" * 56
        print(f"\n  {sep}")
        self._log("RECOVERY", f"Bat dau yeu cau khoi phuc du lieu...")
        print(f"  {sep}")

        successor_node = self.find_successor_with_fallback(self.id + 1)
        if not successor_node or successor_node["id"] == self.node_id:
            self._log("RECOVERY", "Khong tim duoc successor de khoi phuc")
            return

        self._log("RECOVERY", f"Goi GetSnapshot tu Node {successor_node['id']}...")
        try:
            stub = self._get_stub(successor_node)
            response = stub.GetSnapshot(
                kvstore_pb2.GetSnapshotRequest(requester_node_id=self.node_id),
                timeout=GRPC_TIMEOUT
            )
            if response.data:
                recovered = 0
                for key, value in response.data.items():
                    if key not in self.data:
                        self.data[key] = value
                        self._log("RECOVERY", f"NAP  '{key}' = '{value}'")
                        recovered += 1
                    else:
                        self._log("RECOVERY", f"SKIP '{key}' (da co, giu gia tri hien tai)")
                self._log("RECOVERY", f"Hoan thanh: nap {recovered}/{len(response.data)} key tu Node {successor_node['id']}")
                self._print_storage_status()
            else:
                self._log("RECOVERY", f"Khong co du lieu can khoi phuc tu Node {successor_node['id']}")
        except Exception as e:
            self._log("RECOVERY", f"THAT BAI: khong the ket noi Node {successor_node['id']}")

        print(f"  {sep}\n")

    #Tìm successor mới và gửi lại toàn bộ data khi successor cũ vừa chết
    def _re_replicate_to_new_successor(self, dead_node_id):
        """Khi successor vừa chết, gửi lại toàn bộ data sang successor mới."""
        if not self.data:
            return
        new_successor = self.find_successor_with_fallback(self.id + 1)
        if not new_successor or new_successor["id"] == self.node_id:
            return
        self._log("REPLICA", f"Successor Node {dead_node_id} chet → re-replicate {len(self.data)} key --> Node {new_successor['id']}")
        try:
            stub = self._get_stub(new_successor)
            for key, value in self.data.items():
                stub.Put(
                    kvstore_pb2.PutRequest(
                        key=key, value=value,
                        is_replica=True, owner_node_id=self.node_id
                    ),
                    timeout=GRPC_TIMEOUT
                )
                self._log("REPLICA", f"  Re-send '{key}' --> Node {new_successor['id']}")
        except Exception as e:
            self._log("REPLICA", f"THAT BAI khi re-replicate sang successor moi: {e}")

    #Xóa replica khẩn cấp khỏi node đã tạm giữ sau khi successor gốc phục hồi
    def _cleanup_emergency_replicas(self, recovered_node_info):
        """Xóa các replica khẩn cấp đã gửi khi successor chết, nay successor đã phục hồi."""
        if not self.data:
            return
        recovered_id = recovered_node_info["id"]
        # Node đang giữ replica khẩn cấp là successor của recovered node
        recovered_hash = get_hash(recovered_node_info["address"])
        emergency_node = self.find_successor_with_fallback(recovered_hash + 1)
        if not emergency_node or emergency_node["id"] == self.node_id or emergency_node["id"] == recovered_id:
            return
        self._log("REPLICA", f"Don dep replica khan cap tren Node {emergency_node['id']} (Node {recovered_id} da song lai)")
        try:
            stub = self._get_stub(emergency_node)
            for key in list(self.data.keys()):
                stub.Delete(
                    kvstore_pb2.DeleteRequest(key=key, is_replica=True),
                    timeout=GRPC_TIMEOUT
                )
                self._log("REPLICA", f"  Xoa replica khan cap '{key}' tren Node {emergency_node['id']}")
        except Exception as e:
            self._log("REPLICA", f"THAT BAI khi don dep replica khan cap: {e}")

    #Heartbeat Monitor

    def start_heartbeat_monitor(self):
        """Chạy background thread định kỳ kiểm tra node còn sống, tự xử lý failover."""
        def monitor():
            while True:
                time.sleep(HEARTBEAT_INTERVAL)
                sep = "- " * 28
                print(f"\n  {sep}")
                self._log("HEARTBEAT", f"Kiem tra trang thai cac node trong he thong...")

                for node_info in self.all_nodes:
                    node_id = node_info["id"]
                    if node_id == self.node_id:
                        continue

                    with self.failed_nodes_lock:
                        was_failed = node_id in self.failed_nodes

                    is_alive = self._is_node_alive(node_info)

                    if is_alive and not was_failed:
                        self._log("HEARTBEAT", f"Node {node_id}  [OK]  - dang hoat dong")
                    elif is_alive and was_failed:
                        # Node vừa phục hồi → gửi lại replica + demote data đã promote + dọn replica khẩn cấp
                        self._log("HEARTBEAT", f"Node {node_id}  [ONLINE] - da phuc hoi!")
                        successor = self.find_successor_with_fallback(self.id + 1)
                        if successor and successor["id"] == node_id:
                            self._re_replicate_all_to(node_info)
                            self._cleanup_emergency_replicas(node_info)
                        self._demote_promoted_data(node_info)
                    else:
                        self._log("HEARTBEAT", f"Node {node_id}  [DEAD]  - khong phan hoi")
                        # Nếu node vừa mới chết (không phải đã chết từ trước)
                        # và là successor của node hiện tại → re-replicate sang successor mới
                        if not was_failed:
                            normal_successor = self.find_successor(self.id + 1)
                            if normal_successor["id"] == node_id:
                                self._re_replicate_to_new_successor(node_id)

                # Promote replica của các node đang lỗi lên primary
                if self.replica:
                    self._promote_replica_to_primary()

                self._print_storage_status()
                print(f"  {sep}\n")

        heartbeat_thread = threading.Thread(target=monitor, daemon=True)
        heartbeat_thread.start()
        self._log("HEARTBEAT", f"Monitor khoi dong (kiem tra moi {HEARTBEAT_INTERVAL}s)")
