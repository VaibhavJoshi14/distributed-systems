If a new raft node i needs to be created, first create a folder logs_node_i, and create three empty files inside it: dump.txt, logs.txt, metadata.txt.
Next, open the newly created metadata.txt, and write the following:
nodeId: i
currentTerm: 0
votedFor: None
commitLength: 0

--------------------------------------------------------------------------------------------------------------------------

Compiling the protocol buffer (raft.proto) : runs on pip version 23.1.1 only in windows (not newer versions upto 23.3.x for now),
    python -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=. raft.proto

    