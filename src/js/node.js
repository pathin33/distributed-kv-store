const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

// Load proto file
const PROTO_PATH = path.join(__dirname, '../protos/kvstore.proto');
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {});
const kvstore_proto = grpc.loadPackageDefinition(packageDefinition);