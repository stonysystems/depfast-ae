import matplotlib.pyplot as plt
import numpy as np
import processing

def getdata(proto, r):
    # figure5a, raft: 3,5
    # figure6a, copilot: 3
    lat = []
    tput = []
    if proto == "raft":
        data_3, data_5 = processing.figure5a()
        if r == 3:
            for e in data_3:
                lat.append(float(e[1]))
                tput.append(float(e[2]))
        else:
            for e in data_5:
                lat.append(float(e[1]))
                tput.append(float(e[2]))
    elif proto == "copilot":  # copilot
        data_r = processing.figure6a()
        for e in data_r:
            lat.append(float(e[1]))
            tput.append(float(e[2]))
    else: # proto == "copilot.ref":
        data = processing.figure6a_copilot()
        for e in data:
            lat.append(float(e[2][50])/1000.0)
            tput.append(float(e[1]))

    return lat, tput

def plot_lattput(proto, rep, ax, plt_id):
    proto_set = {
        'raft': ['raft'],
        'copilot': ['copilot', 'copilot.ref']
    }

    real_name = {
	'copilot': 'DepFast-Copilot',
	'copilot.ref': 'Copilot',
	'raft': 'DepFast-Raft',
	'etcd': 'etcd',
	'etcd.one': 'etcd (1-core)'
    }

    marker = {
        'copilot': '-x',
        'copilot.ref': '-^',
        'raft': '-x',
        'etcd': '-^',
        'etcd.one': '-v'
    }

    annotate = {
	'copilot': 11,
	'raft': 9
    }

    for p in proto_set[proto]:
        for r in rep:
            lat, tput = getdata(p, r)

            ax.plot(tput, lat, marker[p], label='{} {}-rep'.format(real_name[p], r),
					linewidth=3, ms=6 if p == 'etcd' else 8, markeredgewidth=3)
            try:
                x = tput[annotate[p]]
                y = lat[annotate[p]]
                ax.plot(x, y, 'r*', label='_nolegend_', ms=12, markeredgewidth=3)
            except:
                pass

    ax.set_xlabel('Throughput (op/s)')
    ax.set_ylabel('Med latency (ms)')
    ax.set_xlim(0)
    ax.set_ylim(0)

    ax.legend(frameon=False, fontsize='small')
    ax.set_box_aspect(0.6)
    ax.set_title('{} Latency-Throughput'.format(plt_id), y=-0.45, fontsize=18, fontweight='bold')


if __name__ == "__main__":
    fig, ax = plt.subplots()
    plot_lattput('raft', [3], ax, '')
