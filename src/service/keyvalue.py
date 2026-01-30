import generated.kvstore_pb2_grpc as kvstore_pb2_grpc
import generated.kvstore_pb2  as kvstore_pb2

class KeyValueServicer(kvstore_pb2_grpc.KeyValueServiceServicer):
    def __init__(self,chord_node ):
        self.node = chord_node
    def Put(self, request, context):
        if request.key is not None:
            #gọi hàm put từ chordnode(file xử lí logic chính)
            self.node.put(request.key,request.value)
            return kvstore_pb2.PutResponse(success = True,message = "Đã lưu!")
        else:
            return kvstore_pb2.PutResponse(success = False,message = "Key và value không được để trống")
    
    def Get(self, request, context):
        #gọi hàm get từ chordnode(file xử lí logic chính)
        if request.key is not None:
            value = self.node.get(request.key)
            if value is not None:
                return kvstore_pb2.GetResponse(
                    success=True,
                    message="Lấy dữ liệu thành công",
                    value=value
                )
        else:
            return kvstore_pb2.GetResponse(
                success=False,
                message="Key không tồn tại",
                value=""
            )
    def Delete(self, request, context):
        if request.key is not None:
            #gọi hàm delete từ chordnode(file xử lí logic chính)
            self.node.delete(request.key)
            return kvstore_pb2.DeleteResponse(
                success = True,
                message = "Xóa thành công"
            )   
        else:
            return kvstore_pb2.DeleteResponse(
                success = False,
                message = "Key không tồn tại"
            )