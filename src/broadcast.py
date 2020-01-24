import os
import socket
import sys
import time

import zmq

#include <czmq.h>
PING_PORT_NUMBER = 55555
PING_MSG_SIZE    = 8
PING_INTERVAL    = 1  # Once per second

def broadcast(message):

    # Create UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

    # Ask operating system to let us do broadcasts from socket
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind UDP socket to local port so we can receive pings
    sock.bind(('', PING_PORT_NUMBER))

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    # Send first ping right away
    ping_at = time.time()

    while True:
        timeout = ping_at - time.time()
        if timeout < 0:
            timeout = 0
        try:
            events = dict(poller.poll(1000* timeout))
        except KeyboardInterrupt:
            print('interrupted')
            break

        if time.time() >= ping_at:
            # Broadcast our beacon
            print ('Pinging peers…')
            sock.sendto(message, 0, ('255.255.255.255', PING_PORT_NUMBER))
            ping_at = time.time() + PING_INTERVAL

if __name__ == '__main__':
    broadcast(b'a')