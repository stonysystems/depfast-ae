import math

durations = []
with open('../log/proc-host1.log') as f:
    for line in f:
        if 'time of fsync' in line:
            if line.split(': ')[1] != '':
                durations.append(int(line.split(': ')[1]))

durations.sort()
print(sum(durations)/len(durations))
