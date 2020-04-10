import sys
import os
import asyncio

from util import int2bytes, bytes2int, run
from radio import Radio
from pubsub import Pub, Sub
from free_port import get_free_tcp_port, get_free_tcp_address

class Vertex():
    def __init__(self, path, lst, port, id):
        self.path = path
        self.lst = lst
        self.port = port
        self.id = id
        self.radio = Radio(port, print)
        self.pub = Pub(port)
        self.sub = Sub(port)
        self.heartbeat_started = asyncio.Event()
        self.neigh_hood_check_started = asyncio.Event()
        self.sub_started = asyncio.Event()

    def makeDir(self):
        try:
            os.mkdir(self.path)
        except OSError:
            print ("Creation of the directory %s failed" % path)
        else:
            print ("Successfully created the directory %s" % path)

        for vertex in (self.lst):
            f = open(f'{path}/{vertex}.txt', 'a+')
            f.close() 
    
    async def fradio(self):
        while True:
            asyncio.create_task(self.radio.start())
            print("1 Radio is initialized")
            self.heartbeat_started.set()
            self.neigh_hood_check_started.set()
            await asyncio.sleep(2)

    async def fpub(self):
        while True:
            print("2 pub Sending...")
            self.pub.send('10001', 'ready')
            self.pub.send('10000', 'req')
            await asyncio.sleep(2)
        
    async def heartbeat(self):
        while True:
            await self.heartbeat_started.wait()
            print("3 Heart beat broadcasting...")
            vertex.radio.send(bytes(self.id, 'utf-8'))
            await asyncio.sleep(2)
        
    async def neigh_hood_check(self):
        while True:
            await self.neigh_hood_check_started.wait()
            # asyncio.create_task(self.sub.listen('10001', print))
            print("4 Checking for matches in neigh list")
            self.sub_started.set()
            await asyncio.sleep(2)
        

    async def subs(self):
        while True:
            await self.sub_started.wait()
            print("5 Subscribing.....")
            asyncio.create_task(self.sub.listen('10001', print))
            asyncio.create_task(self.sub.listen('10000', print))
            await asyncio.sleep(2)
        

    async def start(self):
        self.makeDir()
        while True:
            await asyncio.gather(
                self.fradio(),
                self.fpub(),
                self.heartbeat(),
                self.neigh_hood_check(),
                self.subs(),
            ) 

if __name__ == '__main__':
    lis = sys.argv[2:]
    id = sys.argv[2]
    o_path = os.path.abspath(os.path.realpath(sys.argv[1]))
    path = (f'{o_path}/{lis[0]}')
    port = get_free_tcp_port()
    vertex = Vertex(path, lis, port, id)

    try:
        run(
            vertex.start()
        )
    except KeyboardInterrupt:
        print("Exiting...")
        exit()