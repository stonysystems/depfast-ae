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
    elif proto == "copilot" or proto == "copilot-5":  # copilot
        data_r = processing.figure6a()
        for e in data_r:
            lat.append(float(e[1]))
            tput.append(float(e[2]))
    elif proto == "copilot.ref":
        data = processing.figure6a_copilot()
        for e in data:
            lat.append(float(e[2][50])/1000.0)
            tput.append(float(e[1]))
    else: # etcd
        data_3, data_5 = processing.figure5a_etcd()
        if r == 3:
            for e in data_3:
                lat.append(float(e[1]))
                tput.append(float(e[2]))
        else:
            for e in data_5:
                lat.append(float(e[1]))
                tput.append(float(e[2]))

    return lat, tput

def plot_lattput(proto, rep, ax, plt_id):
    # for 5-rep
    IS_5_REP=True
    proto_set = {
        'raft': ['raft', 'etcd'],
        'copilot': ['copilot-5'] if IS_5_REP else ['copilot', 'copilot.ref'],
        'ref-copilot': ['copilot.ref'],
        'etcd': ['etcd']
    }

    real_name = {
        'copilot': 'DepFast-Copilot',
        'copilot.ref': 'Copilot',
        'raft': 'DepFast-Raft',
        'etcd': 'etcd',
        'etcd.one': 'etcd (1-core)',
        'copilot-5': 'DepFast-Copilot'
    }

    marker = {
        'copilot': '-x',
        'copilot-5': '-x',
        'copilot.ref': '-^',
        'raft': '-x',
        'etcd': '-^',
        'etcd.one': '-v'
    }

    annotate = {
        'copilot': 11,
        'copilot-5': 11,
        'raft': 13
    }

    plots=[]
    for p in proto_set[proto]:
        for r in rep:
            lat, tput = getdata(p, r)
            print("(a) ---> ", p, r, "\n", "    lat:", lat, "\n", "    tput:", tput, "\n")
            label='{} {}-rep'.format(real_name[p], r)
            t,=ax.plot(tput, lat, marker[p], label=label,
					        linewidth=3, ms=6 if p == 'etcd' else 8, markeredgewidth=3)
            plots.append([t,label])
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

    if proto == "raft":
        f1=ax.legend([plots[0][0], plots[1][0]], [plots[0][1], plots[1][1]], loc=2, frameon=False, fontsize='small')
        ax.add_artist(f1)
        ax.legend([plots[2][0], plots[3][0]], [plots[2][1], plots[3][1]], loc=4, frameon=False, fontsize='small')
    else:
        ax.legend(frameon=False, fontsize='small')

    ax.set_box_aspect(0.6)
    ax.set_title('{} Latency-Throughput'.format(plt_id), y=-0.45, fontsize=18, fontweight='bold')


if __name__ == "__main__":
    fig, ax = plt.subplots()
    plot_lattput('raft', [3], ax, '')
