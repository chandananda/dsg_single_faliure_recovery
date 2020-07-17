import os
import signal
import sys
import shutil
import datetime
import in_place

import fileinput

process_id = None
node_id = None
f_cache = None

kill_node = sys.argv[1]
print("Node to kill -> ", kill_node)

with open('process_ids.txt', 'r') as file:
    for line in file:
        stripped_line = line.strip()
        print(stripped_line)
        line_info_list = stripped_line.split(':')
        node_id = line_info_list[0]
        process_id = line_info_list[1]
        pid = int(process_id)
        if node_id == kill_node:
            kill_id = pid
            print(f'--------{kill_node} Node will be killed ---------')
            os.kill(pid, signal.SIGTERM)

shutil.move(kill_node, 'copy')
detection_time_start = datetime.datetime.now().time()

with open('kill_time.txt', 'a+') as store_time:
    store_time.write(f'{kill_node}kill_time={detection_time_start}')
    store_time.write("\n")

with in_place.InPlace('process_ids.txt')as file:
    for line in file:
        line = line.replace(f'{kill_node}:{kill_id}', '')
        file.write(line)

fh = open("process_ids.txt", "r")
lines = fh.readlines()
fh.close()
# Weed out blank lines with filter
lines = filter(lambda x: not x.isspace(), lines)
# Write
fh = open("process_ids.txt", "w")
fh.write("".join(lines))
# should also work instead of joining the list:
# fh.writelines(lines)
fh.close()

