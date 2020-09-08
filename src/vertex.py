import sys
import os
import asyncio
import string
import random
import time
import linecache
import operator
import tkinter
import datetime
import psutil

from pubsub import Pub, Sub
from radio import Radio
from util import int2bytes, bytes2int, run
from free_port import get_free_tcp_port, get_free_tcp_address
from pushpull import Push, Pull
from collections import deque
from tkinter import *
from tkinter import messagebox
from datetime import datetime, date

RADIO_PORT = 55555
MSG_TOPIC = '10001'
STR_RANGE = 10
PP_PORT = get_free_tcp_port()
total_broadcast_no = 5
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
        self.node_failure = asyncio.Event()
        self.lost_help_list = []
        self.heartbeat_sense_buff = {}
        self.temp_buff = {}
        self.recovery_time_start = datetime.now()
        self.srecovery = datetime.min.time()

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
        if not self.neighbourhood[1:]:
            self.srecovery = datetime.now().time()
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
                print(f'Sending by PUB: {msg}' )
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
                if self.srecovery != datetime.min.time():
                    recent = datetime.now().time()
                    srecovery_time = datetime.combine(date.today(), recent) - datetime.combine(date.today(), self.srecovery)
                    srecord = open('output.txt', 'a+')
                    srecord.write(f'Self Recovery time = {srecovery_time}')
                    srecord.write('\n')
                    srecord.close()
                    dialog = tkinter.Tk()
                    dialog.withdraw()
                    messagebox.showinfo(f'Self Recovery time', srecovery_time ) 
                    dialog.quit()
                    self.srecovery = datetime.min.time()
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
            print(f'Received Heartbeat from {vertex_id}, {vertex_port} → {msg}')
            print(self.node_failure.is_set)
            if vertex_id in self.heartbeat_sense_buff.keys() and vertex_msg == 'ready':
                self.heartbeat_sense_buff[vertex_id] += 1
                print(f'Heartbeat Buffer: {self.heartbeat_sense_buff}')
            if vertex_msg == 'ready' and vertex_id in self.neighbourhood [1:] and vertex_id not in self.subbed_neighbors:
                self.lost_help_list.clear()
                self.node_failure.clear()
                for id in self.heartbeat_sense_buff:
                    self.heartbeat_sense_buff[id] = 0
                self.heartbeat_sense_buff[vertex_id] = 0
                print(f'Match found from READY msg: {vertex_id}')
                sub = Sub(vertex_port)
                self.subbed_neighbors[vertex_id] = sub
                self.sub_listen_task[vertex_id] = asyncio.create_task(sub.listen(MSG_TOPIC, self.post_msg))
                self.pushed_neighbours[vertex_id] = Push(vertex_pp_port)
                print(f'From neighbourhood watch: READY \n {self.subbed_neighbors} \n {self.pushed_neighbours}')
            elif vertex_msg == 'lost' and vertex_id in self.neighbourhood [1:] and vertex_id not in self.subbed_neighbors and vertex_id not in self.lost_help_list:
                self.recovery_time_start = datetime.now().time()
                self.lost_help_list.append(vertex_id)
                self.recovery_push = Push(vertex_pp_port)
                file = open(f'{self.path}/{vertex_id}.txt', 'r+')
                for line_no, line in enumerate(file, 1):
                    self.recovery_push.send(f'{self.neighbourhood[0]}:{vertex_id}:{line}')
                    time.sleep(2)
                file.truncate(0)
                file.close()
                self_file = open(f'{self.path}/{self.neighbourhood[0]}.txt', 'r')
                for line_no, line in enumerate(self_file, 1):
                    if self.neighbourhood[line_no % (len(self.neighbourhood) - 1) + 1] == vertex_id:
                        line_data = f'{line_no}:{line}'
                        self.recovery_push.send(f'{self.neighbourhood[0]}:{self.neighbourhood[0]}:{line_data}')
                        time.sleep(2)
                self_file.close()
                self.recovery_push.push_cancel()
                recent_time = datetime.now().time()
                recovery_time = datetime.combine(date.today(), recent_time) - datetime.combine(date.today(), self.recovery_time_start)
                window = tkinter.Tk()
                window.withdraw()
                messagebox.showinfo(f'Recovery_time: {self.neighbourhood[0]}', recovery_time ) 
                window.quit()
                record = open('output.txt', 'a+')
                record.write(f'Recovery time by {self.neighbourhood[0]} = {recovery_time}')
                record.write('\n')
                record.close()
                print(f'From neighbourhood watch: LOST \n {self.subbed_neighbors} \n {self.pushed_neighbours}')

    async def failure_detection(self):
        await self.heart_beat_started.wait()
        count = 1
        single_temp = deque(maxlen=4)
        failed_node = None
        while True:
            print(f'from failure detection: {self.node_failure}, {self.node_failure.is_set()}')
            if self.neighbourhood[1:] and self.heartbeat_sense_buff and self.node_failure:
                if len(self.neighbourhood[1:]) == 1 and not self.node_failure.is_set():
                    print(self.heartbeat_sense_buff)
                    print(self.heartbeat_sense_buff.values())
                    val, = self.heartbeat_sense_buff.values()
                    single_temp.append(val)
                    print(self.heartbeat_sense_buff)
                    print(single_temp)
                    if (len(single_temp) == 4) and (single_temp[0] - single_temp[3] == 0):
                        failed_node = self.neighbourhood[1]
                        print(f'Failed id: {failed_node}')
                        self.node_failure.set()
                        single_temp.clear()
                elif len(self.heartbeat_sense_buff.keys()) > 1 and not self.node_failure.is_set():
                    min_id, min_val = min(self.heartbeat_sense_buff.items(), key = lambda x: x[1])
                    max_id, max_val = max(self.heartbeat_sense_buff.items(), key = lambda x: x[1])
                    print(f'From failure detection : {self.heartbeat_sense_buff} -> {min_val}, {max_val}')
                    if (max_val - min_val) > 1:
                        print(f'Failed id: {min_id}')
                        failed_node = min_id
                        self.node_failure.set()
                if failed_node is not None and self.node_failure.is_set():
                    with open('kill_time.txt') as file:
                        for line in file:
                            if f'{failed_node}kill_time' in line:
                                line_info_list = line.split('=')
                                node_id = line_info_list[0]
                                fail_detect_start = line_info_list[1]
                                datetime_obj = datetime.strptime(fail_detect_start, '%H:%M:%S.%f\n').time()
                                current_time = datetime.now().time()
                                detection_time = datetime.combine(date.today(), current_time) - datetime.combine(date.today(), datetime_obj)
                                top = tkinter.Tk()  
                                top.withdraw()
                                messagebox.showinfo(f'Detection: {self.neighbourhood[0]}',detection_time) 
                                top.quit()
                                record = open('output.txt', 'a+')
                                record.write(f'Output records for {failed_node} \n')
                                record.write(f'Detection time by {self.neighbourhood[0]} = {detection_time}')
                                record.write('\n')
                                record.close()
                    self.sub_listen_task.pop(failed_node, None).cancel()
                    self.subbed_neighbors.pop(failed_node, None).sub_cancel()
                    self.pushed_neighbours.pop(failed_node, None).push_cancel()
                    del self.heartbeat_sense_buff[failed_node]
                    failed_node = None
            await asyncio.sleep(5)


    def post_msg(self, payload):
        msg = str(payload[1], 'utf-8')
        print(f'Received msg: {msg}')
        file = open(f'{self.path}/{self.neighbourhood[0]}.txt', 'a+')
        file.write(f'{msg}\n')
        file.close()

    async def partial_replication(self):
        await self.heart_beat_started.wait()
        print('Partial Replication Started')
        self.pull = Pull(self.pp_port)
        asyncio.create_task(self.pull.listen(self.replicate_msg))
        await asyncio.sleep(5)
        line_no = 1
        while True:
            print('partial checking')
            print(f'Neigh: {self.neighbourhood} -> {len(self.neighbourhood[1:])}, push: {len(self.pushed_neighbours.keys())}, sub: {len(self.subbed_neighbors.keys())}')
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
                if round_robin_id in self.pushed_neighbours.keys():
                    self.pushed_neighbours[round_robin_id].send(f'{self.neighbourhood[0]},{line_no},{line}')
                    print(f'Vertex No: {round_robin_id}, data: {line}')
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
        sender_id = message_list[0]
        rec_vertex_id = message_list[1]
        line_no = int(message_list[2])
        line_data = message_list[3]
        print(f'The received message: {message}')
        if rec_vertex_id == self.neighbourhood[0]:
            global COUNTER
            file = open(f'{self.path}/{self.neighbourhood[0]}.txt', 'a+')
            if COUNTER == line_no:
                # file.write(f'{line_no}:{line_data}')
                file.write(f'{line_data}')
                COUNTER += 1
                while COUNTER in self.temp_buff.keys():
                    # file.write(f'{COUNTER}:{self.temp_buff[COUNTER]}')
                    file.write(f'{self.temp_buff[COUNTER]}')
                    COUNTER += 1
            else:
                self.temp_buff[line_no] = line_data
            file.close()

            if sender_id not in self.neighbourhood[1:]:
                self.neighbourhood.append(sender_id)
        else:
            f = open(f'{self.path}/{rec_vertex_id}.txt', 'a+')
            f.write(f'{line_no}:{line_data}')
            f.close()

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
   time.sleep(1)
    pid = os.getpid()
    store_id = open('process_ids.txt', 'a+')
    store_id.write(f'{lis[0]}:{pid}')
    store_id.write("\n")
    store_id.close()
    time.sleep(1)
    process = psutil.Process(os.getpid())
    consumes0 = process.memory_info()[0]
    consumes1 = process.memory_info()[1]
    tcomsumes = (consumes0 + consumes1)
    hui = open("process_consuming.txt", "a+")
    hui.write(f'{lis[0]}-->{tcomsumes}')
    hui.write("\n")
    hui.close()
    try:
        run(
            vertex.start()
        )
    except KeyboardInterrupt:
        print("Exiting...")
        exit()
