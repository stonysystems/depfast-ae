import csv
import matplotlib.pyplot as plt
import numpy as np
import os
import yaml
import lattput
import processing

typ = 'leader'
if processing.is_rw(): # for rw
    CK=220
    D_X=10
    D_T=40
else:
    CK=200
    D_X=10
    D_T=12

num2exp = {
    0: 'No Slowness',
    1: 'CPU Slowness',
    2: 'CPU Contention',
    5: 'Network Slowness',
    6: 'Memory Contention'
}

exp2num = {
    'No Slowness': 0,
    'CPU Slowness': 1,
    'CPU Contention': 2,
    'Disk Slowness': 3,
    'Disk Contention': 4,
    'Network Slowness': 5,
    'Memory Contention': 6
}

metrics = [
    'Throughput (op/s)',
    'Average Latency (ms)',
    'P99 Latency (ms)'
]

reps = [3, 5]
typs = ['follower', 'leader']

def load_process_data(protocol, ty, exp, rep):
    if protocol == "raft":
        if exp == 0:
            data_3, data_5 = processing.figure5a()
            if rep == 3:
                for e in data_3:
                    if e[0] == CK:
                        return e[2], e[1], e[3]
            else:
                for e in data_5:
                    if e[0] == CK:
                        return e[2], e[1], e[3]
        else:
            data_3, data_5 = processing.figure5b()
            if rep == 3:
                for e in data_3:
                    if e[0] == exp:
                        return e[2], e[1], e[3]["99"]
            else:
                for e in data_5:
                    if e[0] == exp:
                        return e[2], e[1], e[3]["99"]
    else:
        if exp == 0:
            data_r = processing.figure6a()
            if ty == "follower":
                for e in data_r:
                    if e[0] == D_T:
                        return e[2], e[1], e[3]
            else:
                for e in data_r:
                    if e[0] == D_T:
                        return e[2], e[1], e[3]
        else:
            data_l, data_f = processing.figure6b()
            if ty == "follower":
                for e in data_f:
                    if e[0] == exp:
                        return e[2], e[1], e[3]["99"]
            else:
                for e in data_l:
                    if e[0] == exp:
                        return e[2], e[1], e[3]["99"]
    return 0, 0, 0



def plot_figure(all_data, metric, ax, plt_id):
    # labels = ['{} Nodes'.format(r) for r in reps]
    labels = typs

    x = np.arange(len(labels))
    width = 0.15

    i = -2
    lines = []
    for n, e in num2exp.items():
        try:
            # slow_res = [all_data['follower'][r][e][metric] for r in reps]
            slow_res = [all_data[t][3][e][metric] for t in typs]
            lines.append(ax.bar(x + i*width, slow_res, width, label=e))
            i += 1
        except:
            continue
    
    ax.set_ylabel(metrics[metric])
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_box_aspect(0.6)
    ax.set_title('{} Throughput with slowness'.format(plt_id), y=-0.45, fontsize=18, fontweight='bold')

    return lines

def get_cdf_data(protocol, ty, exp, rep):
    latency = []
    if protocol == "raft":  
        if exp==0: # no slowness 
            data_3, data_5 = processing.figure5a()
        else:
            data_3, data_5 = processing.figure5b()
        if rep == 3:
            for e in data_3:
                if exp==0:
                    if e[0] == CK:
                        latency = e[4]
                else:
                    if e[0] == exp:
                        latency = e[3]
        else:
            for e in data_5:
                if exp==0:
                    if e[0] == CK:
                        latency = e[4]
                else:
                    if e[0] == exp:
                        latency = e[3]
    else: # copilot
        if exp==0:
            data_r = processing.figure6a()
            data_l, data_f = data_r, data_r
        else:
            data_l, data_f = processing.figure6b()
        if ty == "follower":
            for e in data_f:
                if exp==0:
                    if e[0] == D_T:
                        latency = e[4]
                else:
                    if e[0] == exp:
                        latency = e[3]
        else:
            for e in data_l:
                if exp==0:
                    if e[0] == D_T:
                        latency = e[4]
                else:
                    if e[0] == exp:
                        latency = e[3]
    
    pct_lat = {}
    if not latency:
        return pct_lat

    for k, v in latency.items():
        try:
            pct = round(float(k))
            if pct > 0:
                pct_lat[pct/100] = v
        except:
            continue
    
    return pct_lat


def plot_cdf(all_cdf, ty, rep, ax, plt_id):
    for e in num2exp.values():
        pct_lat = all_cdf[ty][rep][e]
        pct = list(pct_lat.keys())
        lat = list(pct_lat.values())
        pct.sort()
        lat.sort()
        ax.plot(lat, pct, linewidth=3)

    ax.set_ylabel('CDF')
    ax.set_ylim([0, 1])
    ax.set_xlim([0, D_X] if ty == 'leader' else 0)
    ax.set_xlabel('Latency (ms)')
    # ax.set_xscale('log')
    ax.set_box_aspect(0.6)
    ax.set_title('{} CDF ({} slow)'.format(plt_id, ty), y=-0.45, fontsize=18, fontweight='bold')


if __name__ == '__main__':
    protocol = 'copilot'
    all_data = {}
    all_cdf = {}
    for t in typs:
        all_data[t] = {}
        all_cdf[t] = {}
        for r in reps:
            all_data[t][r] = {}
            all_cdf[t][r] = {}
            for n, e in num2exp.items():
                try:
                    one_result = load_process_data(protocol, t, n, r)
                    all_data[t][r][e] = one_result
                except:
                    all_data[t][r][e] = (0, 0, 0)
                cdf = get_cdf_data(protocol, t, exp2num[e], r)
                all_cdf[t][r][e] = cdf

    plt.rcParams['font.size'] = 18
    plt.rcParams['font.family'] = 'serif'
    fig, axes = plt.subplots(1, 4, figsize=(25,4))
    
    print("(b) ----> ", all_data)
    lines = plot_figure(all_data, 0, axes[1], '(b)')
    plot_cdf(all_cdf, 'follower', 3, axes[2], '(c)')
    plot_cdf(all_cdf, 'leader', 3, axes[3], '(d)')

    fig.legend(lines, labels=num2exp.values(), loc='upper center', ncol=len(num2exp), frameon=False)
    lattput.plot_lattput(protocol, [3], axes[0], '(a)')

    plt.subplots_adjust(wspace=0.32)
    
    
    # plt.show()
    fig.savefig(os.path.join(".", 'imgs', 'depfast_{}.pdf'.format(protocol)), bbox_inches='tight')
