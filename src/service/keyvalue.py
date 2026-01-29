import generated.kvstore_pb2_grpc as kvstore_pb2_grpc
import generated.kvstore_pb2  as kvstore_pb2

class KeyValueServicer(kvstore_pb2_grpc.KeyValueServiceServicer):
    def __init__(self,chord_node  ):
        self.node = chord_node
    def Put(self, request, context):
        
        return kvstore_pb2.PutResponse(success = True,message = "Đã lưu!")
    
    def Get(self, request, context):
        return kvstore_pb2.GetResponse(
            success = True ,
            message = "Lấy dữ liệu thành công",
            )
    def Delete(self, request, context):
        return kvstore_pb2.DeleteResponse(
            success = True,
            message = "Xóa thành công"
        )   