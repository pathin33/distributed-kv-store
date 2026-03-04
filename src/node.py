import grpc
import sys
# Dam bao output flush ngay lap tuc, khong bi buffer
sys.stdout.reconfigure(line_buffering=True)
from concurrent import futures
import argparse
import json
import threading
import time
from service.keyvalue import KeyValueServicer
import generated.kvstore_pb2_grpc as kvstore_pb2_grpc
from chord.chordnode import ChordNode

def serve():
    parser = argparse.ArgumentParser(description="Start gRPC node")
    parser.add_argument("--id", type=int, required=True, help="Node ID (e.g. 1, 2, 3)")
    args = parser.parse_args()

    with open("config.json", "r") as f:
        config = json.load(f)

    my_info = next((n for n in config["nodes"] if n["id"] == args.id), None)
    if my_info is None:
        raise ValueError(f"Node id={args.id} khong tim thay trong config.json")

    chord_node = ChordNode(
        node_id=my_info["id"],
        address=my_info["address"],
        all_nodes_config=config["nodes"]
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_KeyValueServiceServicer_to_server(
        KeyValueServicer(chord_node),
        server
    )
    server.add_insecure_port(my_info["address"])
    server.start()

    sep = "=" * 56
    print(f"\n  {sep}")
    print(f"  [Node {args.id} | STARTUP  ] Distributed KV Store")
    print(f"  [Node {args.id} | STARTUP  ] ID      : {args.id}")
    print(f"  [Node {args.id} | STARTUP  ] Address : {my_info['address']}")
    print(f"  [Node {args.id} | STARTUP  ] Hash ID : {chord_node.id}")
    print(f"  [Node {args.id} | STARTUP  ] Nhan Ctrl+C de tat server")
    print(f"  {sep}\n")

    def startup_tasks():
        time.sleep(3)
        chord_node.recover_data_from_successor()
        chord_node.start_heartbeat_monitor()

    threading.Thread(target=startup_tasks, daemon=True).start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n  [Node {args.id} | SHUTDOWN ] Dang tat server...")
        server.stop(0)
        print(f"  [Node {args.id} | SHUTDOWN ] Da tat.")


if __name__ == "__main__":
    serve()
