Run each of the nodes from a terminal in the same directory as the node python files.

Empty each of the metadata.txt for a fresh start of raft nodes. Empty them properly so that only one empty line remains.

The data of each database (of each node <i>) is stored in data/data<i>.txt.


-----------------------------------------------

If a new raft node i needs to be created, first create a folder logs_node_i, and create three empty files inside it: dump.txt, logs.txt, metadata.txt.

--------------------------------------------------------------------------------------------------------------------------

Compiling the protocol buffer (raft.proto) : runs on pip version 23.1.1 only in windows (not newer versions upto 23.3.x for now),
    python -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=. raft.proto

    