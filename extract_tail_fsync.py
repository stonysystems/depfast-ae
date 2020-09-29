import math

durations = []
with open('../log/proc-host2.log') as f:
    for line in f:
        if 'time of fsync' in line:
            if line.split(': ')[1] != '':
                durations.append(int(line.split(': ')[1]))

durations2 = []
with open('../log/proc-host3.log') as f:
    for line in f:
        if 'time of fsync' in line:
            if len(line.split(': ')) > 1:
                durations2.append(int(line.split(': ')[1]))


min_durations = []
length = min(len(durations), len(durations2))
for i in range(length):
    min_durations.append(min(durations[i], durations2[i]))


durations.sort()
durations2.sort()
min_durations.sort()


index = int(math.ceil(99/100*len(durations)))-1
print('99.0 percent tail of follower 1: ', durations[index])
index = int(math.ceil(99.9/100*len(durations)))-1
print('99.9 percent tail of follower 1: ', durations[index])

index = int(math.ceil(99/100*len(durations2)))-1
print('99.0 percent tail of follower 2: ', durations2[index])
index = int(math.ceil(99.9/100*len(durations2)))-1
print('99.9 percent tail of follower 2: ', durations2[index])

index = int(math.ceil(99/100*len(min_durations)))-1
print('99.0 percent tail of minimum: ', min_durations[index])
index = int(math.ceil(99.9/100*len(min_durations)))-1
print('99.9 percent tail of minimum: ', min_durations[index])
print('avg of min: ', sum(min_durations)/len(min_durations))
