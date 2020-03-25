import sys
import os
import asyncio

from pubsub import Pub, Sub
from radio import Radio
from util import int2bytes, bytes2int, run

class Vertex():
    def __init__(self, path, lst, ):
        self.path = path
        self.lst = lst

    def makeDir(self, path, lst):
        try:
            os.mkdir(path)
        except OSError:
            print ("Creation of the directory %s failed" % path)
        else:
            print ("Successfully created the directory %s" % path)

        for vertex in lst:
            f = open(f'{path}/{vertex}.txt', 'a+')
            f.close() 

if __name__ == '__main__':
    lis = sys.argv[2:]
    o_path = os.path.abspath(os.path.realpath(sys.argv[1]))
    path = (f'{o_path}/{lis[0]}')
    v = Vertex(path, lis)
    v.makeDir(path, lis)