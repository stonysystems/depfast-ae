import math
import numpy as np
import os.path
import sys

lats = []
with open('../log/proc-host6.log') as f:
    for line in f:
        if 'commo window avg:' in line:
            if len(line.split(': ')) > 1:
                lats.append(int(line.split(': ')[1]))

cpus = []
first_cpu = 0.0
index = 0
with open('../log/proc-host6.log') as f:
    for line in f:
        if 'cpu: ' in line:
            if line.split(': ')[1] != '':
                after_cpu = line.split(': ')[1]
                if index == 0:
                    index = 1
                    first_cpu = float(after_cpu.split(' ')[0])
                elif float(after_cpu.split(' ')[0]) != first_cpu:
                    cpus.append(float(after_cpu.split(' ')[0]))
                    #print(float(after_cpu.split(' ')[0]))


print(sum(lats)/len(lats))
if os.path.isfile('../latency' + sys.argv[1] + '.npy'):
    lat_arr = np.load('../latency' + sys.argv[1] + '.npy')
else:
    lat_arr = []
lat_arr = np.append(lat_arr, sum(lats)/len(lats))
print(lat_arr.shape)
np.save('../latency' + sys.argv[1] + '.npy', lat_arr)

lats.sort()
#print(avg_lat)
cpus.sort()
print(sum(cpus)/len(cpus))
print(cpus[int(0.1*len(cpus))-1])
if os.path.isfile('../cpus' + sys.argv[1] + '.npy'):
    cpu_arr = np.load('../cpus' + sys.argv[1] + '.npy')
else:
    cpu_arr = []
cpu_arr = np.append(cpu_arr, cpus[int(0.1*len(cpus))-1])
np.save('../cpus' + sys.argv[1] + '.npy', cpu_arr)

print(cpus[int(0.05*len(cpus))-1])
