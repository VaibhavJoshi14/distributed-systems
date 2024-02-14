import sys
import zmq

if __name__ == "__main__":
    if (len (sys.argv) == 2):
        uuid = sys.argv[1]
        success = "SUCCESS".encode ("UTF-8")
        fail = "FAILED".encode ("UTF-8")
        socket = None

        message_server_ip = "localhost"
        message_server_port = 5555
        

        while True:

            print ("--------------------------------")
            print ("| 1 = Get all groups list      |")
            print ("| 2 = Join group               |")
            print ("| 3 = Leave the group          |")
            print ("| 4 = Send a message           |")
            print ("| 5 = Get messages             |")
            print ("| 6 = Exit                     |")
            print ("--------------------------------")
            try:
                cmd = int (input ())
                if (cmd == 1):
                    try:
                        context = zmq.Context()
                        socket = context.socket(zmq.REQ)
                        socket.connect(f"tcp://{message_server_ip}:{message_server_port}")

                        request = f"USER {uuid}"
                        socket.send (request.encode ("UTF-8"))

                        response = socket.recv ().decode ().split (",")
                        length = int (response[0])
                        for i in range (length):
                            print (response[i+1])
                        socket.close ()
                    except:
                        print("UNABLE TO CONNECT TO MESSAGE SERVER")
                elif cmd in [2,3,4,5]:
                    ip = input ("Group IP: ")
                    port = input ("Group port: ")
                    try:
                        context = zmq.Context()
                        socket = context.socket(zmq.REQ)
                        socket.connect(f"tcp://{ip}:{port}")
                        socket.RCVTIMEO = 10000
                        
                        if (cmd == 2):
                            request = f"JOIN {uuid}"
                            socket.send (request.encode ("UTF-8"))

                            try:
                                response = socket.recv ()
                            except:
                                response = None

                            if response == success:
                                print ("SUCCESS")
                            else:
                                print ("FAILED")
                                socket = None


                        elif (cmd == 3):
                                request = f"LEAVE {uuid}"
                                socket.send (request.encode ("UTF-8"))

                                try:
                                    response = socket.recv ()
                                except:
                                    response = None

                                if response == success:
                                    print ("SUCCESS")

                                else:
                                    print ("FAILED")                            

                        elif (cmd == 4):
                            message = input ("Message: ")
                            request = f"SEND {uuid} {message}"
                            socket.send (request.encode ("UTF-8"))

                            try:
                                response = socket.recv ()
                            except:
                                response = None

                            if response == success:
                                print ("SUCCESS")
                            else:
                                print ("FAILED")

                        elif (cmd == 5):
                            print("Time format HH:MM:SS with 24 hour format")
                            time = input ("Time : ")

                            if time == "*":
                                time = "00::00::00"

                            request = f"GET {uuid} {time}"
                            socket.send (request.encode ("UTF-8"))

                            response = socket.recv ().decode ().split (",")
                            length = int (response[0])
                            for i in range (length):
                                print (response[i+1])

                        if socket != None:
                            socket.close ()
                    except:
                        print("GROUP UNAVAILABLE")
                        
                elif (cmd == 6):
                    break

            except:
                print("PLEASE ENTER VALID COMMAND FROM LIST")
                
    else:
        print ("Usage: python3 user.py <uuid>")
        exit ()