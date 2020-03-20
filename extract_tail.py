import math

durations = []
with open('proc-p1-fast.log') as f:
    for line in f:
        if 'Duration of Wait()' in line:
            durations.append(int(line.split(': ')[1]))

durations.sort()

index = int(math.ceil(99.99/100*len(durations)))-1
print(durations[index])


durations2 = []
with open('proc-p1-slow.log') as f:
    for line in f:
        if 'Duration of Wait()' in line:
            durations2.append(int(line.split(': ')[1]))

durations2.sort()

index = int(math.ceil(99.99/100*len(durations2)))-1
print(durations2[index])
print(sum(durations)/len(durations))
print(sum(durations2)/len(durations2))
