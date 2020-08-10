import math

durations = []
with open('log/proc-host3.log') as f:
    for line in f:
        if 'Time for Write' in line:
            if len(line.split(': ')) > 1:
                durations.append(int(line.split(': ')[1]))

durations.sort()

print(len(durations))
index = int(math.ceil(99.9/100*len(durations)))-1
print('99.9 percent tail of fast: ', durations[index])
index2 = int(math.ceil(99.99/100*len(durations)))-1
print('99.99 percent tail of fast: ', durations[index2])
index3 = int(math.ceil(99.999/100*len(durations)))-1
print('99.999 percent tail of fast: ', durations[index3])
