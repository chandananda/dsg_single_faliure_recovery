import sys
import os
import csv
import subprocess

file_path = os.path.realpath(sys.argv[1])

reader = csv.reader(open(file_path), delimiter=',', quotechar=' ')

dictionary = {}

for row in reader:
    x = row[0]
    y = row[1]
    dictionary.setdefault(x, []).append(y)
    dictionary.setdefault(y, []).append(x)

for vertex in dictionary:
    neigh = ' '.join(dictionary[vertex])
    subprocess.call(f'python ./vertex.py {vertex} {neigh}', shell=True)
    # subprocess.call('python pi.txt', shell=True)
    # Run: D:\Educational\dsg_single_faliure_recovery\src> python .\controller.py ..\data\facebook_data.csv