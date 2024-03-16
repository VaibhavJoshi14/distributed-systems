Compiling the protocol buffer (raft.proto) : runs on pip version 23.1.1 only in windows (not newer versions upto 23.3.x for now),
    python -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=. raft.proto

    