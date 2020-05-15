import sys
import os
import asyncio
import string
import random
import time
import linecache
import operator

from pubsub import Pub, Sub
from radio import Radio
from util import int2bytes, bytes2int, run
from free_port import get_free_tcp_port, get_free_tcp_address
from pushpull import Push, Pull

RADIO_PORT = 55555
MSG_TOPIC = '10001'
STR_RANGE = 10
PP_PORT = get_free_tcp_port()
total_broadcast_no = 5
LINE = 1
COUNTER = 1

class Vertex():
    def __init__(self, path, neighbourhood):
        self.path = path
        self.neighbourhood = neighbourhood
        self.port = get_free_tcp_port()
        self.pp_port = get_free_tcp_port()
        self.radio_started = asyncio.Event()
        self.pub_started = asyncio.Event()
        self.heart_beat_started = asyncio.Event()
        self.subbed_neighbors = {}
        self.pushed_neighbours = {}
        self.sub_listen_task = {}
        self.getting_ready_to_write = asyncio.Event()
        self.has_finished_writing = asyncio.Event()
        self.node_failure = asyncio.Event()
        self.lost_help_list = []
        self.heartbeat_sense_buff = {}
        self.temp = {}
        self.lock = asyncio.Lock()

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
            if len(self.neighbourhood[1:]) is len(self.subbed_neighbors.keys()) and self.neighbourhood[1:]:
                chars = string.ascii_uppercase + string.ascii_lowercase
                msg = ''.join(random.choice(chars) for _ in range(STR_RANGE))
                print(f'Sending: {msg}' )
                self.pub.send(MSG_TOPIC, msg)
            await asyncio.sleep(5 * len(self.neighbourhood[1:]))

    async def init_heart_beat(self):
        await self.radio_started.wait()
        await self.pub_started.wait()
        self.heart_beat_started.set()
        while True:
            if not self.neighbourhood[1:]:       
                self.recovery_pull = Pull(PP_PORT)
                asyncio.create_task(self.recovery_pull.listen(self.gather_msg))
                for broadcasting_no in range(total_broadcast_no):
                    msg = f'lost,{self.port},{PP_PORT},{self.neighbourhood[0]}'
                    self.radio.send(bytes(msg, 'utf-8'))
                    print(f"LOST msg broadcasting: {msg}")
                    await asyncio.sleep(5)
                """Shift recovery pull cancel"""
                # self.recovery_pull.pull_cancel()
            else:
                msg = f'ready,{self.port},{self.pp_port},{self.neighbourhood[0]}'
                self.radio.send(bytes(msg, 'utf-8'))
                print(f'Heart beat broadcasting: {self.port}, {self.neighbourhood[0]}')
                await asyncio.sleep(5)

    def neighbourhood_watch(self, msg, addr, port):
        if self.neighbourhood[1:]:
            str_msg = str(msg, 'utf-8')
            msg_list = str_msg.split(',')
            vertex_msg = msg_list[0]
            vertex_port = msg_list[1]
            vertex_pp_port = msg_list[2]
            vertex_id = msg_list[3]
            print(f'Received Heartbeat from {vertex_id} : {vertex_port} → {msg} : {vertex_msg}')
            print(self.node_failure.is_set)
            # if vertex_id in self.heartbeat_sense_buff.keys() and not self.node_failure.is_set() and vertex_msg == 'ready':
            if vertex_id in self.heartbeat_sense_buff.keys() and vertex_msg == 'ready':
                self.heartbeat_sense_buff[vertex_id] += 1
                print(self.heartbeat_sense_buff)
            if vertex_msg == 'ready' and vertex_id in self.neighbourhood [1:] and vertex_id not in self.subbed_neighbors:
                self.lost_help_list.clear()
                self.node_failure.clear()
                for id in self.heartbeat_sense_buff:
                    self.heartbeat_sense_buff[id] = 0
                self.heartbeat_sense_buff[vertex_id] = 0
                print(f'Match found from ready msg {vertex_id}')
                sub = Sub(vertex_port)
                self.subbed_neighbors[vertex_id] = sub
                task = asyncio.create_task(sub.listen(MSG_TOPIC, self.post_msg))
                self.sub_listen_task[vertex_id] = task
                push = Push(vertex_pp_port)
                print(self.pushed_neighbours)
                self.pushed_neighbours[vertex_id] = push
                print(self.pushed_neighbours)
                print(f'From neighbourhood watch msg = ready {self.subbed_neighbors}{self.pushed_neighbours}')
            elif vertex_msg == 'lost' and vertex_id in self.neighbourhood [1:] and vertex_id not in self.subbed_neighbors and vertex_id not in self.lost_help_list:
                self.lost_help_list.append(vertex_id)
                self.recovery_push = Push(vertex_pp_port)
                file = open(f'{self.path}/{vertex_id}.txt', 'r+')
                for line_no, line in enumerate(file, 1):
                    self.recovery_push.send(f'{self.neighbourhood[0]}:{line}')
                    time.sleep(2)
                file.truncate(0)
                file.close()
                self.recovery_push.push_cancel()
                print(f'From neighbourhood watch msg = lost {self.subbed_neighbors}{self.pushed_neighbours}')

    async def failure_detection(self):
        await self.heart_beat_started.wait()
        count = 1
        single_temp = {}
        failed_node = None
        while True:
            print(f'from failure detection: {self.node_failure}, {self.node_failure.is_set()}')
            if self.neighbourhood[1:] and self.heartbeat_sense_buff and self.node_failure:
                temp = self.heartbeat_sense_buff
                if len(temp.keys()) == 1 and not self.node_failure.is_set():
                    val, = temp.values()
                    single_temp[count] = val
                    if count > 1 and single_temp[count] - single_temp[count - 1] > 0:
                        failed_node = id
                        self.node_failure.set()
                elif len(temp.keys()) > 1 and not self.node_failure.is_set():
                    min_id, min_val = min(temp.items(), key = lambda x: x[1])
                    max_id, max_val = max(temp.items(), key = lambda x: x[1])
                    print(f'From failure detection : {temp} -> {min_val}, {max_val}')
                    if (max_val - min_val) > 1:
                        print(f'Failed id: {min_id}')
                        failed_node = min_id
                        self.node_failure.set()
                if failed_node is not None and self.node_failure.is_set():
                    task_instance = self.sub_listen_task.pop(failed_node, None)
                    task_instance.cancel()
                    print('task deleted')
                    print(self.subbed_neighbors)
                    instance = self.subbed_neighbors.pop(failed_node, None)
                    instance.sub_cancel()
                    print('sub deleted')
                    print(self.subbed_neighbors)
                    print(f'failed out: {self.lock.locked()}')
                    await self.lock.acquire()
                    try:
                        print(f'failed in: {self.lock.locked()}')
                        print(self.pushed_neighbours)
                        print('step 1')
                        pInstance = self.pushed_neighbours.pop(failed_node, None)
                        print(pInstance)
                        print('step 2')
                        print(self.pushed_neighbours)
                        print('step 3')
                        pInstance.push_cancel()
                        print('push deleted')
                        print(self.pushed_neighbours)
                    finally:
                        self.lock.release()
                    del self.heartbeat_sense_buff[failed_node]
                    failed_node = None
                    print('heartbeat deleted')
            await asyncio.sleep(5)


    def post_msg(self, payload):
        msg = str(payload[1], 'utf-8')
        print(f'Received msg: {msg}')
        f = open(f'{self.path}/{self.neighbourhood[0]}.txt', 'a+')
        f.write(f'{msg}\n')
        f.close()

    async def partial_replication(self):
        await self.heart_beat_started.wait()
        print('Partial Replication Started')
        self.pull = Pull(self.pp_port)
        for id in self.neighbourhood[1:]:
            if os.path.isfile(f'{self.path}/{id}.txt'):
                with open(f'{self.path}/{id}.txt','w'): pass
        asyncio.create_task(self.pull.listen(self.replicate_msg))
        await asyncio.sleep(5)
        line_no = 1
        while True:
            print('partial checking')
            if self.neighbourhood[1:] and len(self.neighbourhood[1:]) == len(self.pushed_neighbours.keys()) and len(self.neighbourhood[1:]) == len(self.subbed_neighbors.keys()):
                print('hi i am in')
                file = f'{self.path}/{self.neighbourhood[0]}.txt'
                while file is None:
                    await asyncio.sleep(2)
                line = linecache.getline(file, line_no)
                while not line:
                    await asyncio.sleep(2)
                    linecache.clearcache()
                    line = linecache.getline(file, line_no)
                    print(f'New line: {line}')
                round_robin_id = self.neighbourhood[line_no % (len(self.neighbourhood) - 1) + 1]
                round_robin_no = line_no % (len(self.neighbourhood) - 1) + 1
                print(f'round robin no: {round_robin_id} -> {round_robin_no}')
        
                print(self.lock.locked())
                await self.lock.acquire()
                try:
                    print(self.lock.locked())
                    if round_robin_id in self.pushed_neighbours.keys():
                        self.pushed_neighbours[round_robin_id].send(f'{self.neighbourhood[0]},{line_no},{line}')
                        print(f'Vertex No: {round_robin_id}, data: {line}')
                finally:
                    self.lock.release()

                line_no = line_no + 1
            await asyncio.sleep(0)
        
    def replicate_msg(self, rec_payload):
        message = str(rec_payload, 'utf-8')
        message_list = message.split(',')
        rec_vertex_id = message_list[0]
        line_no = message_list[1]
        line_data = message_list[2]
        file = open(f'{self.path}/{rec_vertex_id}.txt', 'a+')
        file.write(f'{line_no}:{line_data}')
        file.close()

    def gather_msg(self, message):
        message = str(message, 'utf-8')
        message_list = message.split(':')
        rec_vertex_id = message_list[0]
        line_no = int(message_list[1])
        line_data = message_list[2]
        print(f'The received message: {message}')
        global COUNTER
        file = open(f'{self.path}/{self.neighbourhood[0]}.txt', 'a+')
        print(f'Entering gather_msg: {COUNTER}')
        if COUNTER == line_no:
            file.write(f'{line_no}: {line_data}')
            COUNTER += 1
            print(f'write from if: {COUNTER} {self.temp}')
            while COUNTER in self.temp.keys():
                file.write(f'{COUNTER}: {self.temp[COUNTER]}')
                COUNTER += 1
                print(f'write from while: {COUNTER}')
        else:
            print(f'from else: {COUNTER} {self.temp}')
            self.temp[line_no] = line_data
        file.close()

        if rec_vertex_id not in self.neighbourhood[1:]:
            self.neighbourhood.append(rec_vertex_id)

    async def start(self):
        self.makeDir()
        await asyncio.gather(
            self.init_radio(),
            self.init_pub(),
            self.init_heart_beat(),
            self.partial_replication(),
            self.failure_detection(),
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
