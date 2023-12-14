import math
import matplotlib.pyplot as plt
import numpy as np
import sys, getopt
import subprocess
import csv

def calculate_latency_metrics(file, concurrent, req, conflict):
    latencies = []
    with open('./plots/epaxos/' + file + '_' + concurrent + '_' + req + '_' + conflict + '.csv', 'a') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for row in spamreader:
            for index in range(0, len(row)-1):
                latencies.append(float(row[index]))
    latencies.sort()
    latencies = [ x * 1000 for x in latencies ]

    print('Time consumed (ms) - p50:    ', round(np.percentile(latencies, 50), 6))
    print('Time consumed (ms) - p90:    ', round(np.percentile(latencies, 90), 6))
    print('Time consumed (ms) - p99:    ', round(np.percentile(latencies, 99), 6))
    print('Time consumed (ms) - p99.9:  ', round(np.percentile(latencies, 99.9), 6))
    print('Time consumed (ms) - p99.99: ', round(np.percentile(latencies, 99.99), 6))
    print('Time consumed (ms) - max:    ', round(latencies[-1], 6))
    print('Time consumed (ms) - avg:    ', round(np.average(latencies), 6))

    # Plot histogram
    a = np.array(latencies)
    plt.clf()
    plt.hist(a, bins = 10000) 
    plt.xlabel('Time (in ms)')
    plt.ylabel('Count')
    plt.savefig('plots/epaxos/' + file + '_' + concurrent + '_' + req + '_' + conflict + '.png')

    # Plot percentile
    plt.clf()
    percentiles = [np.percentile(latencies, p) for p in range(101)]
    plt.plot(percentiles)
    plt.xlabel('Percentile')
    plt.ylabel('Time (in ms)')
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