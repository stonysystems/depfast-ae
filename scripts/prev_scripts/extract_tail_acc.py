import math

durations = []
with open('log/proc-host1.log') as f:
    for line in f:
        if 'The duration of Accept() for 2' in line:
            temp = line.split(': ')
            if len(temp) == 2:
                durations.append(int(temp[1]))

durations.sort()

index = int(math.ceil(99.9/100*len(durations)))-1
print(durations[index:], len(durations))


durations2 = []
with open('proc-p3-slow.log') as f:
    for line in f:
        if 'Duration of Accept()' in line:
            durations2.append(int(line.split(': ')[1]))

durations2.sort()

index = int(math.ceil(99.99/100*len(durations2)))-1
print(durations2[index])
print(sum(durations)/len(durations))
print(sum(durations2)/len(durations2))
