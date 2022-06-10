import csv
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import os
import yaml
import lattput
import processing

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

num2exp = {
    0: 'No Slowness',
    1: 'CPU Slowness',
    2: 'CPU Contention',
    3: 'Disk Slowness',
    4: 'Disk Contention',
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
typs = ['follower']


def plot_figure(all_data, metric, ax, plt_id):
    labels = ['{} Nodes'.format(r) for r in reps]
    # labels = typs

    x = np.arange(len(labels))
    width = 0.1

    i = -3
    lines = []
    for n, e in num2exp.items():
        try:
            slow_res = [all_data['follower'][r][e][metric] for r in reps]
            # ONLY for slowness error bar
            err_res = [all_data['follower'][r][e][3] for r in reps]
            # slow_res = [all_data[t][3][e][metric] for t in typs]
            lines.append(ax.bar(x + i*width, slow_res, width, label=e, yerr=err_res,
                                                                       align='center',
                                                                       ecolor='black',
                                                                       capsize=2))
            i += 1
        except:
            continue
    
    ax.set_ylabel(metrics[metric])
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_box_aspect(0.6)
    ax.set_title('{} Throughput with slowness'.format(plt_id), y=-0.45, fontsize=18, fontweight='bold')

    return lines

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
    ax.set_xlim([0, 45])
    ax.set_xlabel('Latency (ms)')
    # ax.set_xscale('log')
    ax.set_box_aspect(0.6)
    ax.set_title('{} CDF ({} Nodes)'.format(plt_id, rep), y=-0.45, fontsize=18, fontweight='bold')


if __name__ == '__main__':
    protocol = 'raft'
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
                    one_result = processing.load_process_data(protocol, t, n, r)
                    all_data[t][r][e] = one_result
                except:
                    all_data[t][r][e] = (0, 0, 0, 0)
                    raise
                cdf = processing.get_cdf_data(protocol, t, exp2num[e], r)
                all_cdf[t][r][e] = cdf

    plt.rcParams['font.size'] = 18
    plt.rcParams['font.family'] = 'serif'
    fig, axes = plt.subplots(1, 4, figsize=(25,4), gridspec_kw={
        'width_ratios': [1,1,1,1]
    })
    
    import json, pprint
    pp = pprint.PrettyPrinter(indent=4)
    print("(b) ---> ")
    pp.pprint(all_data)
    
    lines = plot_figure(all_data, 0, axes[1], '(b)')
    plot_cdf(all_cdf, 'follower', 3, axes[2], '(c)')
    plot_cdf(all_cdf, 'follower', 5, axes[3], '(d)')

    fig.legend(lines, labels=num2exp.values(), loc='upper center', ncol=len(num2exp), frameon=False, fontsize='small')
    lattput.plot_lattput(protocol, [3, 5], axes[0], '(a)')

    plt.subplots_adjust(wspace=0.32)
    
    
    # plt.show()
    fig.savefig(os.path.join(".", 'imgs', 'depfast_{}.pdf'.format(protocol)), bbox_inches='tight')
