import os
import socket
import asyncio
import zmq

from util import int2bytes, bytes2int, run
from free_port import get_free_tcp_port, get_free_tcp_address


class Radio():
    def __init__(self, port, listener, size=24, poll=1000):
        self.port = port
        self.listener = listener
        self.size = size
        self.poll = poll

        self.socket = socket.socket(
            socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(('', port))

        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    async def start(self):
        while True:
            events = dict(self.poller.poll(self.poll))
            if self.socket.fileno() in events:
                message, socket = self.socket.recvfrom(self.size)
                self.listener(message, socket[0], socket[1])
            await asyncio.sleep(0)

    def send(self, message):
        self.socket.sendto(message, 0, ('255.255.255.255', self.port))
        return


if __name__ == '__main__':
    radio = Radio(get_free_tcp_port(), lambda m, a, p: print(
        f"{a}:{p} → {bytes2int(m):08b}"))

    async def send():
        while True:
            print("Sending...")
            radio.send(int2bytes(0b00011000))
            await asyncio.sleep(5)

    try:
        run(
            radio.start(),
            send(),
        )
    except KeyboardInterrupt:
        print("Exiting...")
        exit()
