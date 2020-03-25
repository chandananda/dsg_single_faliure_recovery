import sys
import os
import socket

# tcp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# tcp.connect(("8.8.8.8", 80))
# print(tcp.getsockname()[0])
# tcp.close()

# print(socket.gethostbyname(socket.gethostname()))

# Getting a random free tcp port in python using sockets

def get_free_tcp_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port


def get_free_tcp_address():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    host, port = tcp.getsockname()
    tcp.close()
    return 'tcp://{host}:{port}'.format(**locals())
