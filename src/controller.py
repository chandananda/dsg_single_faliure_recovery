import sys
import os
import csv

file_path = os.path.realpath(sys.argv[1])

reader = csv.reader(open(file_path), delimiter=',', quotechar=' ')

dog = {}

for row in reader:
    x = row[0]
    y = row[1]
    dog.setdefault(x, []).append(y)
    dog.setdefault(y, []).append(x)

for vertex in dog:
    neigh = ' '.join(dog[vertex])
    print(f'python vertex.py {vertex} {neigh}')