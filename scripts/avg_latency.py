import math

avg_lat = 0
with open('log/proc-host4.log') as f:
    for line in f:
        if 'commo total avg:' in line:
            if len(line.split(': ')) > 1:
                avg_lat = int(line.split(': ')[1])

durations = []
with open('log/proc-host4.log') as f:
    for line in f:
        if 'cpu: ' in line:
            if line.split(': ')[1] != '':
                after_cpu = line.split(': ')[1]
                durations.append(float(after_cpu.split(' ')[0]))

durations.sort()
print(avg_lat)
print(sum(durations)/len(durations))
print(durations[0])
