import math

durations = []
with open('log/proc-host1.log') as f:
    for line in f:
        if 'Time for Async_Accept() for 0' in line:
            if len(line.split(': ')) > 1:
                durations.append(int(line.split(': ')[1]))

durations.sort()

index = int(math.ceil(99.9/100*len(durations)))-1
print('Matrix after tail for fast case:')
print(durations[index:])


durations2 = []
with open('../slow_host2/log/proc-host1.log') as f:
    for line in f:
        if 'Time for Async_Accept() for 0' in line:
            if line.split(': ')[1] != '':
                durations2.append(int(line.split(': ')[1]))

durations2.sort()

index = int(math.ceil(99.99/100*len(durations2)))-1
print()
print('Matrix after tail for slow case:')
print(durations2[index])
print(sum(durations)/len(durations))
print(sum(durations2)/len(durations2))
