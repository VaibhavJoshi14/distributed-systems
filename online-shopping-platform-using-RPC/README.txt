
Compiling the protocol buffer (market.proto) : runs on pip version 23.1.1 only in windows (not newer versions upto 23.3.x for now),
    python -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=. market.proto

For every service provided by "market", the seller and buyer run each service at least once in their programs.

The programs DO NOT handle mutual exclusion which is important for it to be meaningful. To handle this issue presently, 
the server only runs ONE worker, which ensures that race conditions do not happen. If more than one workers are required for concurrency, then 
use some mutual exclusion primitives.

Set the market IP as the 'Externel IP' shown in VM instance created for running the market server. This changes every time instance is started.
Market listens on port 50051.
Two VMs are created in google cloud. 1 vm runs market server. The other vm's 4 terminal instances run 2 clients and 2 sellers.

Run in this order
    python market.py in one terminal
    python seller.py in another terminal
    python seller2.py in another term
    python client.py in 3rd terminal
    python client2.py in another term

