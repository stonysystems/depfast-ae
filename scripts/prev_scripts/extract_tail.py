import math

durations = []
with open('../fast_b/log/proc-host1.log') as f:
    for line in f:
        if 'Duration of Wait()' in line:
            if len(line.split(': ')) > 1:
                durations.append(int(line.split(': ')[1]))

durations.sort()

index = int(math.ceil(99.9/100*len(durations)))-1
print('99.9 percent tail of fast: ', durations[index])
index = int(math.ceil(99.99/100*len(durations)))-1
print('99.99 percent tail of fast: ', durations[index])


durations2 = []
with open('../slow_host2/log/proc-host1.log') as f:
    for line in f:
        if 'Duration of Wait()' in line:
            if line.split(': ')[1] != '':
                durations2.append(int(line.split(': ')[1]))

durations2.sort()

index = int(math.ceil(99.9/100*len(durations2)))-1
print('99.9 percent tail of slow: ', durations2[index])
index = int(math.ceil(99.99/100*len(durations2)))-1
print('99.99 percent tail of slow: ',durations2[index])
print(sum(durations)/len(durations))
print(sum(durations2)/len(durations2))
