import math
import matplotlib.pyplot as plt
import numpy as np
import sys, getopt
import subprocess

def calculate_latency_metrics(file, concurrent, req, conflict):
    latencies = []
    with open('./plots/epaxos/' + file + '.log') as f:
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
    plt.xlabel('Time (in sec)')
    plt.ylabel('Count')
    plt.savefig('plots/epaxos/' + file + '_' + concurrent + '_' + req + '_' + conflict + '.png')

    # Plot percentile
    plt.clf()
    percentiles = [np.percentile(latencies, p) for p in range(101)]
    plt.plot(percentiles)
    plt.xlabel('Percentile')
    plt.ylabel('Time (in sec)')
    plt.savefig('plots/epaxos/' + file + '_percentile' + '_' + concurrent + '_' + req + '_' + conflict + '.png') 

def main(argv):
    inputfile = ''
    outputfile = ''
    opts, args = getopt.getopt(argv,'n:T:o:')
    cmd = ['build/deptran_server', '-f', 'config/epaxos_perf_test.yml']
    for opt, arg in opts:
        if opt == '-n':
            cmd.append('-n')
            cmd.append(arg)
            concurrent = arg
        elif opt == '-T':
            cmd.append('-T')
            cmd.append(arg)
            req = arg
        elif opt == '-o':
            cmd.append('-o')
            cmd.append(arg)
            conflict = arg
    print('Executing command ' + ' '.join(cmd))
    with open('out.log', 'w') as outfile:
        subprocess.run(cmd, stdout=outfile)
    print('\nMaximum execution latency metrics')
    calculate_latency_metrics('max_latencies', concurrent, req, conflict)
    print('\nMinimum execution latency metrics')
    calculate_latency_metrics('min_latencies', concurrent, req, conflict)

if __name__ == '__main__':
    main(sys.argv[1:])