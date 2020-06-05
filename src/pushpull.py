import sys
import zmq
import asyncio
import zmq.asyncio


class Pull():
    def __init__(self, port, addr='*'):
        self.port = port
        self.addr = addr

        self.ctx = zmq.asyncio.Context()
        self.socket = self.ctx.socket(zmq.PULL)
        self.socket.bind(f'tcp://{self.addr}:{self.port}')

    async def listen(self, listener):
        while True:
            string = await self.socket.recv()
            listener(string)

    def pull_cancel(self):
        self.socket.close()
        self.ctx.term()

class Push():
    def __init__(self, port, addr='localhost'):
        self.port = port
        self.addr = addr

        self.ctx = zmq.Context()
        self.scoket = self.ctx.socket(zmq.PUSH)
        self.scoket.connect(f'tcp://{self.addr}:{self.port}')

    def send(self, message):
        self.scoket.send(bytes(message, 'utf-8'))
        print(f'sending:{message}')

    def push_cancel(self):
        self.scoket.setsockopt(zmq.LINGER, 1)
        self.scoket.close()
        self.ctx.term()