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
        radio = Radio(self.port, print)
        self.radio_started.set()
        print("1 Radio Started")
        await asyncio.sleep(20000000)

    async def init_pub(self):
        pub = Pub(self.port)
        print('2 Pub Started')
        self.pub_started.set()
        await asyncio.sleep(2)

    async def init_heart_beat(self):
        await self.radio_started.wait()
        await self.pub_started.wait()
        while True:
            print("3 Heart beat broadcasting")
            await asyncio.sleep(2)

    def init_neighbourhood_watch(self):
        print("4 neighbourhood watch running")

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
    vertex = Vertex(path, lis)

    try:
        run(
            vertex.start()
        )
    except KeyboardInterrupt:
        print("Exiting...")
        exit()