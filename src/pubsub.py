# pylint: disable=maybe-no-member
import sys
import asyncio
import zmq
import zmq.asyncio

from util import int2bytes, bytes2int, run


class Pub():
    def __init__(self, port, addr='*'):
        self.addr = addr
        self.port = port

        context = zmq.Context()
        self.socket = context.socket(zmq.PUB)
        self.socket.bind(f'tcp://{self.addr}:{self.port}')

    def send(self, topic, message):
        self.socket.send_multipart([
            bytes(topic, 'utf8'),
            bytes(message, 'utf8')
        ])

    def pub_cancel(self):
        self.socket.close()

class Sub():
    def __init__(self, port, addr='localhost'):
        self.addr = addr
        self.port = port

        context = zmq.asyncio.Context()
        self.socket = context.socket(zmq.SUB)
        self.socket.connect(f'tcp://{self.addr}:{self.port}')

    async def listen(self, topic, listener):
        self.socket.setsockopt(zmq.SUBSCRIBE, bytes(topic, 'utf8'))
        while True:
            string = await self.socket.recv_multipart()
            listener(string)
    
    def sub_cancel(self):
        self.socket.close()

if __name__ == '__main__':
    pub = Pub('55501')

    async def send():
        while True:
            await asyncio.sleep(5)
            print("Sending...")
            pub.send('10001', 'hullo')

    sub = Sub('55501')

    try:
        run(
            sub.listen('10001', print),
            sub.listen('10000', print),
            send(),
        )
    except KeyboardInterrupt:
        print("Exiting...")
        exit()
