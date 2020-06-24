import math

avg_lat = 0
with open('../log/proc-host4.log') as f:
    for line in f:
        if 'commo total_avg:' in line:
            if len(line.split(': ')) > 1:
                avg_lat = int(line.split(': ')[1])

cpus = []
first_cpu = 0.0
index = 0
with open('../log/proc-host4.log') as f:
    for line in f:
        if 'cpu: ' in line:
            if line.split(': ')[1] != '':
                after_cpu = line.split(': ')[1]
                if index == 0:
                    index = 1
                    first_cpu = float(after_cpu.split(' ')[0])
                elif float(after_cpu.split(' ')[0]) != first_cpu:
                    cpus.append(float(after_cpu.split(' ')[0]))

print(avg_lat)
cpus.sort()
print(sum(cpus)/len(cpus))
print(cpus[int(0.1*len(cpus))-1])
