import sys
import os
import asyncio

from pubsub import Pub, Sub
from radio import Radio
from util import int2bytes, bytes2int, run
from free_port import get_free_tcp_port, get_free_tcp_address

class Vertex():
    def __init__(self, path, neighbourhood):
        self.path = path
        self.neighbourhood = neighbourhood
        self.port = get_free_tcp_port()
        self.radio_started = asyncio.Event()
        self.pub_started = asyncio.Event()
        self.trace_list = []

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
        self.radio = Radio(55555, self.neighbourhood_watch)
        self.radio_started.set()
        print(f"1 Radio Started {self.port}, self id: {self.neighbourhood[0]}, neighbour list: {self.neighbourhood[1:]}")
        await self.radio.start()

    async def init_pub(self):
        self.pub = Pub(self.port)
        self.pub_started.set()
        print('2 Pub Started')
        self.pub.send('10001', 'hullo')
        await asyncio.sleep(0)

    async def init_heart_beat(self):
        await self.radio_started.wait()
        await self.pub_started.wait()
        while True:
            msg = f'ready,{self.port},{self.neighbourhood[0]}'
            self.radio.send(bytes(msg, 'utf-8'))
            print(f"3 Heart beat broadcasting {self.port}, {self.neighbourhood[0]}")
            await asyncio.sleep(2)

    def neighbourhood_watch(self, msg, addr, port):
        str_msg = str(msg, 'utf-8')
        msg_list = str_msg.split(',')
        vertex_msg = msg_list[0]
        vertex_port = msg_list[1]
        vertex_id = msg_list[2]
        # print(self.trace_list)
        # self.trace_list = []
        # print(self.neighbourhood[0])
        # print(self.neighbourhood[1:])
        # print(vertex_msg)
        if vertex_msg == 'ready':
            for vertex in self.neighbourhood[1:]:
                # print(f'list vertex: {vertex}, vertex ID: {vertex_id}')
                if vertex_id not in self.trace_list:
                    if vertex == vertex_id:
                        print(f'match found {vertex}')
                        self.trace_list.append(vertex_id)
                        self.sub = Sub(vertex_port)
                        break
                else:
                    print('nothing')
        # print(f"ID: {vertex_id} : Broadcast {addr} + {port} : Self {vertex_port} → {bytes2int(bytes(msg_list[0], 'utf-8')):08b}")

    def post_msg(self, payload):
        return

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