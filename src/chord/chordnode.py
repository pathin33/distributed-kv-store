import hashlib
import grpc
import sys
sys.stdout.reconfigure(line_buffering=True)
import generated.kvstore_pb2 as kvstore_pb2
import generated.kvstore_pb2_grpc as kvstore_pb2_grpc
import threading
import time

M = 7          # key chay tu 0 den 2^M - 1
RING_SIZE = 2 ** M
GRPC_TIMEOUT = 2          # Timeout cho moi gRPC call (giay)
HEARTBEAT_INTERVAL = 5    # Kiem tra heartbeat moi 5 giay
MAX_RETRY_NODES = 3       # So node toi da thu khi forward


def get_hash(key):
    h = hashlib.sha1(key.encode()).hexdigest()
    return int(h, 16) % RING_SIZE


#  Helper log: moi dong log deu bat dau bang tag
#  [Node X | CATEGORY]  de de loc trong terminal
def _tag(node_id, category):
    """Tao prefix tag chuan: [Node X | CATEGORY]"""
    return f"[Node {node_id} | {category:<9}]"


class ChordNode:

    def __init__(self, node_id, address, all_nodes_config):
        self.node_id = node_id
        self.address = address
        self.all_nodes = all_nodes_config
        self.id = get_hash(address)

        self.successor = self
        self.predecessor = self

        self.data = {}       # du lieu chinh ma node chiu trach nhiem
        self.replica = {}    # ban sao backup (key -> (value, owner_id))
        self.node_stubs = {} # cache stub {node_id: stub}

        self.failed_nodes = set()
        self.failed_nodes_lock = threading.Lock()
    #  Internal helpers

    def _get_stub(self, node_info):
        node_id = node_info["id"]
        if node_id in self.node_stubs:
            return self.node_stubs[node_id]
        channel = grpc.insecure_channel(node_info["address"])
        stub = kvstore_pb2_grpc.KeyValueServiceStub(channel)
        self.node_stubs[node_id] = stub
        return stub

    def _log(self, category, msg):
        """In mot dong log co dinh dang chuan"""
        print(f"{_tag(self.node_id, category)} {msg}")

    def _print_storage_status(self):
        """In bang trang thai data va replica sau moi thao tac"""
        replica_display = {k: v for k, (v, _) in self.replica.items()}
        sep = "-" * 56
        print(f"\n  {sep}")
        print(f"  {_tag(self.node_id, 'STORAGE')} Trang thai hien tai")
        print(f"  {sep}")
        print(f"  {'Data (chinh):':<18} {self.data if self.data else '(trong)'}")
        print(f"  {'Replica (sao):':<18} {replica_display if replica_display else '(trong)'}")
        print(f"  {sep}\n")

    #  Chord routing

    def find_successor(self, key_id):
        """Tim node chiu trach nhiem key_id (khong loc failed)."""
        node_ids = sorted(
            [(get_hash(n["address"]), n) for n in self.all_nodes],
            key=lambda x: x[0]
        )
        for node_hash, node_info in node_ids:
            if key_id <= node_hash:
                return node_info
        return node_ids[0][1]  # quay vong

    def find_successor_with_fallback(self, key_id, exclude_nodes=None):
        """Tim successor bo qua cac node da failed hoac trong exclude_nodes."""
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

    #  CRUD operations

    def put(self, key, value):
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

    #  Failure detection & marking

    def _mark_node_as_failed(self, node_id):
        with self.failed_nodes_lock:
            if node_id not in self.failed_nodes:
                self.failed_nodes.add(node_id)
                self._log("FAILOVER", f"Node {node_id} duoc danh dau FAILED")
                self._invalidate_stub(node_id)

    def _invalidate_stub(self, node_id):
        if node_id in self.node_stubs:
            del self.node_stubs[node_id]
            self._log("FAILOVER", f"Da xoa stub cache cua Node {node_id}")

    def _is_node_alive(self, node_info):
        node_id = node_info["id"]
        if node_id == self.node_id:
            return True
        try:
            stub = self._get_stub(node_info)
            stub.Ping(kvstore_pb2.PingRequest(), timeout=GRPC_TIMEOUT)
            # Ping thanh cong: neu truoc do bi danh dau failed thi xoa di
            with self.failed_nodes_lock:
                if node_id in self.failed_nodes:
                    self.failed_nodes.discard(node_id)
                    self._invalidate_stub(node_id)
            return True
        except Exception:
            self._mark_node_as_failed(node_id)
            return False

    #  Replication

    def _replicate_to_successor(self, key, value):
        successor_node = self.find_successor_with_fallback(self.id + 1)
        if successor_node and successor_node["id"] != self.node_id:
            try:
                stub = self._get_stub(successor_node)
                stub.Put(
                    kvstore_pb2.PutRequest(
                        key=key,
                        value=value,
                        is_replica=True,
                        owner_node_id=self.node_id
                    ),
                    timeout=GRPC_TIMEOUT
                )
                self._log("REPLICA", f"Sao chep '{key}' --> Node {successor_node['id']}")
            except Exception as e:
                self._log("REPLICA", f"THAT BAI khi sao chep '{key}': {e}")

    def _delete_replica_from_successor(self, key):
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

    def _re_replicate_all_to(self, node_info):
        """Gui lai toan bo data chinh duoi dang replica toi node vua phuc hoi."""
        if not self.data:
            return
        target_id = node_info["id"]
        self._log("REPLICA", f"Re-replicate {len(self.data)} key --> Node {target_id} (vua phuc hoi)")
        try:
            stub = self._get_stub(node_info)
            for key, value in self.data.items():
                stub.Put(
                    kvstore_pb2.PutRequest(
                        key=key,
                        value=value,
                        is_replica=True,
                        owner_node_id=self.node_id
                    ),
                    timeout=GRPC_TIMEOUT
                )
                self._log("REPLICA", f"  Re-send '{key}' --> Node {target_id}")
        except Exception as e:
            self._log("REPLICA", f"THAT BAI khi re-replicate --> Node {target_id}: {e}")

    #  Promote replica -> primary

    def _promote_replica_to_primary(self):
        """Promote replica cua cac node da chet thanh primary data."""
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
        """Chuyen cac key da promote (tu node vua phuc hoi) tro lai thanh replica."""
        recovered_id = node_info["id"]
        demoted = []

        for key, value in list(self.data.items()):
            # Kiem tra xem key nay co thuc su thuoc ve node vua phuc hoi khong
            owner = self.find_successor(get_hash(key))
            if owner["id"] == recovered_id:
                # Chuyen tu data (promoted) -> replica
                self.replica[key] = (value, recovered_id)
                del self.data[key]
                demoted.append(key)
                self._log("FAILOVER", f"DEMOTE '{key}' tu primary -> replica (Node {recovered_id} da song lai)")

        if demoted:
            self._log("FAILOVER", f"Da demote {len(demoted)} key, tra lai chu so huu Node {recovered_id}")
            self._print_storage_status()

    #  Forward with retry

    def _forward_with_retry(self, operation, key, value=None, max_retries=MAX_RETRY_NODES):
        key_id = get_hash(key)
        exclude_nodes = set()

        for attempt in range(max_retries):
            owner = self.find_successor_with_fallback(key_id, exclude_nodes)

            if owner is None:
                self._log(operation, f"THAT BAI ({attempt+1} lan): khong con node nao xu ly '{key}'")
                return None

            # Xu ly local (node nay chinh la owner sau fallback)
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
                self._log(operation, f"LOI khi goi Node {owner['id']}: node co the da chet")
                self._mark_node_as_failed(owner["id"])
                exclude_nodes.add(owner["id"])

        self._log(operation, f"THAT BAI sau {max_retries} lan thu cho '{key}'")
        return None
    #  Data recovery (khi node restart)

    def recover_data_from_successor(self):
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

    #  Heartbeat monitor

    def start_heartbeat_monitor(self):
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
                        self._log("HEARTBEAT", f"Node {node_id}  [ONLINE] - da phuc hoi!")
                        # Neu node vua phuc hoi la successor cua minh:
                        # 1. Gui lai replica de no khoi phuc vai tro luu replica
                        # 2. Xoa zombie data (da duoc promote) khoi chinh minh
                        successor = self.find_successor_with_fallback(self.id + 1)
                        if successor and successor["id"] == node_id:
                            self._re_replicate_all_to(node_info)
                        self._demote_promoted_data(node_info)
                    else:
                        self._log("HEARTBEAT", f"Node {node_id}  [DEAD]  - khong phan hoi")

                # Promote replica neu co node failed
                if self.replica:
                    self._promote_replica_to_primary()

                # In trang thai luu tru hien tai de de quan sat
                self._print_storage_status()

                print(f"  {sep}\n")

        heartbeat_thread = threading.Thread(target=monitor, daemon=True)
        heartbeat_thread.start()
        self._log("HEARTBEAT", f"Monitor khoi dong (kiem tra moi {HEARTBEAT_INTERVAL}s)")
