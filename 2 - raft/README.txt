Run each of the nodes (nodei.py for i in [0..4]) from a terminal in the same directory as the node python files.

For a fresh start of raft nodes, delete the files inside logs_node_i for each i, but 
do not delete the directories.

The data of each database (of each node <i>) is stored in data/data<i>.txt. These files can also be deleted for a fresh start.

A menu-driven client is provided in clientMenu.py.
-----------------------------------------------

If a new raft node i needs to be created, first create a folder logs_node_i, and create three empty files inside it: dump.txt, logs.txt, metadata.txt.

-------------------------------------------------------
Implementation details:
    * Leader sends heartbeat every second.
    * Leader sends a lease duration of 3 seconds.
    * Election timeout is set to a random number in 15 to 21 seconds
    * The details of implementation follow the Martin Kleppmann's lecture provided.

--------------------------------------------------------------------------------------------------------------------------

Compiling the protocol buffer (raft.proto) : runs on pip version 23.1.1 only in windows (not newer versions upto 23.3.x for now),
    python -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=. raft.proto

    