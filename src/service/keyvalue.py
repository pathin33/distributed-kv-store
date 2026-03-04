import generated.kvstore_pb2_grpc as kvstore_pb2_grpc
import generated.kvstore_pb2 as kvstore_pb2
from chord.chordnode import get_hash

class KeyValueServicer(kvstore_pb2_grpc.KeyValueServiceServicer):
    def __init__(self, chord_node):
        self.node = chord_node

    def _tag(self, category):
        return f"[Node {self.node.node_id} | {category:<9}]"

    def Put(self, request, context):
        if not request.key:
            return kvstore_pb2.PutResponse(success=False, message="Key khong duoc trong")
        if request.is_replica:
            self.node.replica[request.key] = (request.value, request.owner_node_id)
            print(f"{self._tag('REPLICA')} Nhan replica '{request.key}' = '{request.value}' (owner: Node {request.owner_node_id})")
            self.node._print_storage_status()
        else:
            self.node.put(request.key, request.value)
        return kvstore_pb2.PutResponse(success=True, message="OK")

    def Get(self, request, context):
        if not request.key:
            return kvstore_pb2.GetResponse(success=False, message="Key khong duoc trong", value="")
        value = self.node.get(request.key)
        if value is not None:
            return kvstore_pb2.GetResponse(success=True, message="OK", value=value)
        else:
            return kvstore_pb2.GetResponse(success=False, message="Key khong ton tai", value="")

    def Delete(self, request, context):
        if not request.key:
            return kvstore_pb2.DeleteResponse(success=False, message="Key khong duoc trong")
        if request.is_replica:
            if request.key in self.node.replica:
                del self.node.replica[request.key]
                print(f"{self._tag('REPLICA')} Xoa replica '{request.key}'")
                self.node._print_storage_status()
                return kvstore_pb2.DeleteResponse(success=True, message="Xoa replica OK")
            else:
                return kvstore_pb2.DeleteResponse(success=False, message="Replica khong ton tai")
        else:
            success = self.node.delete(request.key)
            if success:
                return kvstore_pb2.DeleteResponse(success=True, message="Xoa OK")
            else:
                return kvstore_pb2.DeleteResponse(success=False, message="Key khong ton tai")

    def GetSnapshot(self, request, context):
        requester_id = request.requester_node_id
        print(f"{self._tag('RECOVERY')} Nhan GetSnapshot tu Node {requester_id}")
        snapshot = {}

        # Nguon 1: replica chua duoc promote (van con trong replica dict)
        for key, (value, owner_id) in self.node.replica.items():
            if owner_id == requester_id:
                snapshot[key] = value

        # Nguon 2: data da duoc promote tu replica (bi xoa khoi replica khi promote)
        # => Kiem tra theo consistent hash: key nao thuc su thuoc ve requester_id?
        for key, value in self.node.data.items():
            owner = self.node.find_successor(get_hash(key))
            if owner["id"] == requester_id and key not in snapshot:
                snapshot[key] = value

        print(f"{self._tag('RECOVERY')} Tra ve {len(snapshot)} key cho Node {requester_id}: {list(snapshot.keys())}")
        return kvstore_pb2.GetSnapshotResponse(data=snapshot)

    def Ping(self, request, context):
        return kvstore_pb2.PingResponse(alive=True)
