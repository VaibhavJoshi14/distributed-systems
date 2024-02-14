import zmq

if __name__ == "__main__":
    success = "SUCCESS".encode ("UTF-8")
    failed = "FAILED".encode ("UTF-8")
    groups = {}

    try:
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://*:{5555}")
        print(f"SERVER STARTED AT PORT {5555}")

    except:
        print("SERVER INITIALIZATION FAILED")


    while True:
        message = socket.recv ().decode ("UTF-8").split ()
        response = failed
        try:
            if message[0] == "GROUP":
                print (f"JOIN REQUEST FROM {message[2]}:{message[3]}")
                groups[message[1]] = f"{message[2]} {message[3]}"
                response = success
            
            elif message[0] == "USER":
                print (f"GROUP LIST REQUEST FROM {message[1]}")
                temp = []
                for key in groups.keys ():
                    temp.append (f"{key} - {groups[key]}")
                
                response = f"{len(temp)},{','.join (temp)}".encode ("UTF-8")
        except:
            response = failed
            print("OPERATION FAILED")
        
        socket.send(response)




