import sys
import zmq

port = "55501"

def sub(address, message):
    if len(sys.argv) > 1:
        port =  sys.argv[1]
        int(port)

    # Socket to talk to server
    context = zmq.Context()
    socket = context.socket(zmq.SUB)

    print("Collecting updates from weather server...")
    socket.connect ("tcp://localhost:%s" % port)

    # Subscribe to zipcode, default is NYC, 10001
    socket.setsockopt(zmq.SUBSCRIBE, b'10001')

    while True:
        string = socket.recv_multipart()
        print(string)

