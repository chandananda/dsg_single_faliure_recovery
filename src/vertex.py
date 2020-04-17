import sys
import os
import asyncio
import string
import random

from pubsub import Pub, Sub
from radio import Radio
from util import int2bytes, bytes2int, run
from free_port import get_free_tcp_port, get_free_tcp_address

RADIO_PORT = 55555
MSG_TOPIC = '10001'
STR_RANGE = 10

class Vertex():
    def __init__(self, path, neighbourhood):
        self.path = path
        self.neighbourhood = neighbourhood
        self.port = get_free_tcp_port()
        self.radio_started = asyncio.Event()
        self.pub_started = asyncio.Event()
        self.subbed_neighbors = {}

    def makeDir(self):
        try:
            os.mkdir(self.path)
        except OSError:
            print ("Creation of the directory %s failed" % path)
        else:
            print ("Successfully created the directory %s" % path)

        for vertex in (self.neighbourhood):
            f = open(f'{path}/{vertex}.txt', 'a+')
            f.close() 

    async def init_radio(self):
        self.radio = Radio(RADIO_PORT, self.neighbourhood_watch)
        self.radio_started.set()
        print(f"1 Radio Started {self.port}, self id: {self.neighbourhood[0]}, neighbour list: {self.neighbourhood[1:]}")
        await self.radio.start()

    async def init_pub(self):
        self.pub = Pub(self.port)
        self.pub_started.set()
        print('2 Pub Started')
        while True:
            chars = string.ascii_uppercase + string.ascii_lowercase
            msg = ''.join(random.choice(chars) for _ in range(STR_RANGE))
            print(f'Sending: {msg}' )
            self.pub.send(MSG_TOPIC, msg)
            await asyncio.sleep(5)

    async def init_heart_beat(self):
        await self.radio_started.wait()
        await self.pub_started.wait()
        while True:
            msg = f'ready,{self.port},{self.neighbourhood[0]}'
            self.radio.send(bytes(msg, 'utf-8'))
            # print(f"3 Heart beat broadcasting {self.port}, {self.neighbourhood[0]}")
            await asyncio.sleep(2)

    def neighbourhood_watch(self, msg, addr, port):
        str_msg = str(msg, 'utf-8')
        msg_list = str_msg.split(',')
        vertex_msg = msg_list[0]
        vertex_port = msg_list[1]
        vertex_id = msg_list[2]
        # print(f'Received Heartbeat from {vertex_id} : {vertex_port} → {msg} : {vertex_msg}')
        if vertex_msg == 'ready' and vertex_id in self.neighbourhood [1:] and vertex_id not in self.subbed_neighbors:
            print(f'match found {vertex_id}')
            sub = Sub(vertex_port)
            asyncio.create_task(sub.listen(MSG_TOPIC, self.post_msg))
            self.subbed_neighbors[vertex_id] = sub
        elif vertex_msg == 'lost':
            print('recovery process')
        # print(f"ID: {vertex_id} : Broadcast {addr} + {port} : Self {vertex_port} → {bytes2int(bytes(msg_list[0], 'utf-8')):08b}")

    def post_msg(self, payload):
        msg = str(payload[1], 'utf-8')
        print(f'received msg: {msg}')
        f = open(f'{self.path}/{self.neighbourhood[0]}.txt', 'a+')
        f.write(f'{msg}\n')
        f.close()

    async def start(self):
        self.makeDir()
        await asyncio.gather(
            self.init_radio(),
            self.init_pub(),
            self.init_heart_beat(),
        )

    
if __name__ == '__main__':
    lis = sys.argv[2:]
    o_path = os.path.abspath(os.path.realpath(sys.argv[1]))
    path = (f'{o_path}/{lis[0]}')
    print('my path is:' + path)
    vertex = Vertex(path, lis)

    try:
        run(
            vertex.start()
        )
    except KeyboardInterrupt:
        print("Exiting...")
        exit()