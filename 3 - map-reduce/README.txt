

Compile the proto file using: runs on pip version 23.1.1 only in windows
    python -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=. map-reduce-kmeans.proto