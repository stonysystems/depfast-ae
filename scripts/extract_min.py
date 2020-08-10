import math

durations = []
durations1 = []
durations2 = []
with open('log/proc-host1.log') as f:
    for line in f:
        if 'Time for AE for 1' in line:
            if len(line.split(': ')) > 1:
                durations1.append(int(line.split(': ')[1]))
        if 'Time for AE for 2' in line:
            if len(line.split(': ')) > 1:
                durations2.append(int(line.split(': ')[1]))
length = min(len(durations1), len(durations2))

for i in range(length):
    durations.append(min(durations1[i], durations2[i]))

durations.sort()
durations1.sort()
durations2.sort()

index = int(math.ceil(99.9/100*len(durations1)))-1
print('99.9 percent tail of AE to follower 1: ', durations1[index])
index2 = int(math.ceil(99.99/100*len(durations1)))-1
print('99.99 percent tail of AE to follower 1: ', durations1[index2])

index = int(math.ceil(99.9/100*len(durations2)))-1
print('99.9 percent tail of AE to follower 2: ', durations2[index])
index2 = int(math.ceil(99.99/100*len(durations2)))-1
print('99.99 percent tail of AE to follower 2: ', durations2[index2])

index = int(math.ceil(99.9/100*len(durations)))-1
print('99.9 percent tail of AE: ', durations[index])
index2 = int(math.ceil(99.99/100*len(durations)))-1
print('99.99 percent tail of AE: ', durations[index2])
