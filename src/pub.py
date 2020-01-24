import zmq
import random
import sys
import time
import socket

# port = "55501"
class Pub:

    def __init__(self):
        self.port = get_free_tcp_port()
        self.address = socket.gethostbyname(socket.gethostname())

        if len(sys.argv) > 1:
            port =  sys.argv[1]
            int(self.port)

        context = zmq.Context()
        sock = context.socket(zmq.PUB)
        sock.bind("tcp://*:%s" % self.port)

    def send_msg(self):
        while True:
            

    def display(self):
        print(self.port, '  ', self.address)


def get_free_tcp_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port

pub = Pub()

if __name__ == '__main__':
    pub.display()