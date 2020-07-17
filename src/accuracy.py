import itertools
import os
import sys
import shutil
import in_place

Name_of_node = sys.argv[1]
Compare_with = "copy" 
count = 0
total = 0

files = os.listdir(Name_of_node)
print(files)
for f in files:
    print(f)
    with open(f'{Compare_with}/{f}') as f1, open(f'{Name_of_node}/{f}') as f2:
        for lineno, (line1, line2) in enumerate(itertools.zip_longest(f1, f2), 1):
            total = total + 1
            if line1 != line2 and line1 != None:
                print(f'Mismatch of file {f} in line no: ', lineno)
                print(f'{line1}  {line2}')
                count = count + 1
print("Total lines & mismatched lines: ", total, count)
accuracy = ((total - count)/total)*100
print('Accuracy rate -----> ', round(accuracy,4))
record = open('Output.txt', 'a+')
record.write(f'Accuracy Rate of recovery = {accuracy}')
record.write('\n------------------------------------------------------------\n')
record.close()
shutil.rmtree('copy')
with open('kill_time.txt') as f:
    for line in f:
        if f'{Name_of_node}kill_time' in line:
            line_info_list = line.split('=')
            node_id = line_info_list[0]
            fail_detect_start = line_info_list[1]
with in_place.InPlace('kill_time.txt')as file:
    for line in file:
        line = line.replace(f'{Name_of_node}kill_time={fail_detect_start}', '')
        file.write(line)
