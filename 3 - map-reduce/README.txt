For creating a new mapper, create a mapper<i>.py file (copy from existing), and 
provide it an ip-port distinct. Update this ip-port on master.py also. Create a directory M<i>
inside directory Mappers.

For creating a new reducer, create a reducer<i>.py file, and provide it a 
distinct ip-port. Add this ip-port on master.py also.
--------------------------------------------------------------------------------------------------------------
Execution instruction:
 
The mappers (mapper<i>.py for i in [1..x]), 
    reducers (reducer<i>.py for i in [1..y])
    master (master.py)
need to be run in separate terminals. Run them in the order specified above.

--------------------------------------------------------------------------------------------------------------
Compile the proto file using: runs on pip version 23.1.1 only in windows
    python -m grpc_tools.protoc -I./ --python_out=. --pyi_out=. --grpc_python_out=. map-reduce-kmeans.proto