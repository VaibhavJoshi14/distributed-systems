from datetime import datetime
import zmq
import pytz
import sys


if __name__ == "__main__":
    if (len (sys.argv) == 4):
        name = sys.argv[1]
        self_ip = sys.argv[2]
        self_port = sys.argv[3]
        success = "SUCCESS".encode ("UTF-8")
        fail = "FAILED".encode ("UTF-8")
        IST = pytz.timezone ("Asia/Kolkata")
        active_users = []
        messages = []

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://localhost:{5555}")

        request = f"GROUP {name} {self_ip} {self_port}".encode ("UTF-8")
        socket.send (request)

        response = socket.recv ()
        if response == success:
            print ("SUCCESS")
            socket.close ()
        else:
            print ("FAILED")
            socket.close ()
            exit ()
        try:
            context = zmq.Context()
            socket = context.socket(zmq.REP)
            socket.bind(f"tcp://*:{self_port}")
        except:
            print("BIND FAILED")
    
        while True:
            message = socket.recv ().decode ("UTF-8").split ()
            response = fail

            if message[0] == "JOIN":
                    print(f"JOIN REQUEST FROM {message[1]}")
                    if (message[1] not in active_users):
                        active_users.append (message[1])
                        response = success
                    else:
                        print(f"{message[1]} ALREADY IN {name}")
                        response = fail
                    print(active_users)

            elif message[0] == "LEAVE":
                    print(f"LEAVE REQUEST FROM {message[1]}")
                    if message[1] in active_users:
                        active_users.remove (message[1])
                        response = success
                    else:
                        response = fail
                    print(active_users)

            elif message[0] == "SEND":
                chat = " ".join (message[2:])
                print (f"MESSAGE SEND FROM {message[1]}")

                if (message[1] in active_users):
                    messages.append ([datetime.now (IST).time (), chat])
                    response = success
                else:
                    response = fail
                print (messages)

            elif message[0] == "GET":
                print (f"MESSAGE REQUEST FROM {message[1]}")

                if message[1] not in active_users:
                    response = "0,".encode ("UTF-8")
                else:
                    try:
                        time = datetime.strptime(message[2], '%H::%M::%S').time()
                    except:
                        time = datetime.strptime("00::00::00", '%H::%M::%S').time()
                    print(time)
                    response = []
                    for temp_chat in messages:
                        if (temp_chat[0] >= time):
                            response.append (temp_chat[1])

                    length = len (response)
                    response = f"{length},{','.join (response)}".encode ("UTF-8")
            socket.send (response)



    else:
        print("please give full command as follows:")
        print ("python3 group.py <name> <ip> <port>")
        exit ()