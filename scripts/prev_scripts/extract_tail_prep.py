import math

durations = []
with open('../log/proc-host4.log') as f:
    for line in f:
        if 'time of prepare wait' in line:
            if line.split(': ')[1] != '':
                durations.append(int(line.split(': ')[1]))

durations.sort()

index = int(math.ceil(99/100*len(durations)))-1
print('99.0 percent tail of follower 1: ', durations[index])
index = int(math.ceil(99.9/100*len(durations)))-1
print('99.9 percent tail of follower 1: ', durations[index])
print('average: ', sum(durations)/len(durations))
