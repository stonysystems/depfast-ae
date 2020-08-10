import math

durations = []
with open('log/proc-host1.log') as f:
    for line in f:
        if 'Time for Wait' in line:
            if len(line.split(': ')) > 1:
                durations.append(int(line.split(': ')[1]))

durations.sort()

index = int(math.ceil(99.0/100*len(durations)))-1
print('99.0 percent tail of fast: ', durations[index])
index2 = int(math.ceil(99.9/100*len(durations)))-1
print('99.9 percent tail of fast: ', durations[index2])
index3 = int(math.ceil(50.0/100*len(durations)))-1
print('50.0 percent tail of fast: ', durations[index3])
