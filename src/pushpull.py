import sys
import zmq
import asyncio
import zmq.asyncio


class Pull():
    def __init__(self, port, addr='*'):
        self.port = port
        self.addr = addr

        ctx = zmq.asyncio.Context()
        self.socket = ctx.socket(zmq.PULL)
        self.socket.bind(f'tcp://{self.addr}:{self.port}')

    async def listen(self, listener):
        while True:
            string = await self.socket.recv()
            listener(string)

class Push():
    def __init__(self, port, addr='localhost'):
        self.port = port
        self.addr = addr

        ctx = zmq.Context()
        self.scoket = ctx.socket(zmq.PUSH)
        self.scoket.connect(f'tcp://{self.addr}:{self.port}')

    def send(self, message):
        self.scoket.send(bytes(message, 'utf-8'))
        print(f'sending:{message}')