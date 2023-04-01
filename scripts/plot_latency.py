import math
import matplotlib.pyplot as plt
import numpy as np


def calculate_latency_metrics(file):
    latencies = []
    with open('./' + file + '.log') as f:
        for line in f:
            if len(line.split(': ')) >= 1:
                latencies.append(float(line.split(':')[0]))
    latencies.sort()

    print('Time consumed - p90:    ', round(np.percentile(latencies, 90), 6))
    print('Time consumed - p99:    ', round(np.percentile(latencies, 99), 6))
    print('Time consumed - p99.9:  ', round(np.percentile(latencies, 99.9), 6))
    print('Time consumed - p99.99: ', round(np.percentile(latencies, 99.99), 6))
    print('Time consumed - max:    ', round(latencies[-1], 6))
    print('Time consumed - avg:    ', round(np.average(latencies), 6))

    # Plot histogram
    a = np.array(latencies)
    plt.clf()
    plt.hist(a, bins = 10000) 
    plt.xlabel("Time (in sec)")
    plt.ylabel("Count")
    plt.savefig(file + '.png')

    # Plot percentile
    plt.clf()
    percentiles = [np.percentile(latencies, p) for p in range(101)]
    plt.plot(percentiles)
    plt.xlabel("Percentile")
    plt.ylabel("Time (in sec)")
    plt.savefig(file + '_percentile.png') 

print('Maximum execution latency metrics')
calculate_latency_metrics('max_latencies')
print('\nMinimum execution latency metrics')
calculate_latency_metrics('min_latencies')