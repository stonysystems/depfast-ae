import yaml
import json
import os
import os.path
from functools import lru_cache
import csv

def readtxt(name):
    if os.path.exists(name):
        with open(name, 'r') as file:
            return [e.replace("\n", "") for e in file.readlines()]
    else:
        return []

def is_rw():
    return int(readtxt("../ips/is_rw")[0])

# the library for the data-processing
IS_5_REP=True
BASE="/home/xuhao/depfast/" if IS_5_REP else "../" # for 5-rep
if is_rw(): # for rw
    SK="WRITE"
else:
    SK="PAYMENT"

# trials, by default: 1
FIGURE5a_TARIALS=3
FIGURE5b_TARIALS=3
FIGURE6a_TARIALS=3
FIGURE6b_TARIALS=3

if is_rw(): # for rw
    CK=340
    D_T=10
else:
    CK=200
    D_T=12

@lru_cache(maxsize=None)
def load_process_data(protocol, ty, exp, rep):  # (tps, 50th, 99th, tps-var)
    if protocol == "raft":
        if exp == 0:
            data_3, data_5 = figure5a()
            if rep == 3:
                for e in data_3:
                    if e[0] == CK:
                        return e[2], e[1], e[3], e[5]
            else:
                for e in data_5:
                    if e[0] == CK:
                        return e[2], e[1], e[3], e[5]
        else:
            data_3, data_5 = figure5b()
            if rep == 3:
                for e in data_3:
                    if e[0] == exp:
                        return e[2], e[1], e[3]["99"], e[4]
            else:
                for e in data_5:
                    if e[0] == exp:
                        return e[2], e[1], e[3]["99"], e[4]
    elif protocol == "copilot":  # copilot
        if exp == 0:
            data_r = figure6a()
            if ty == "follower":
                for e in data_r:
                    if e[0] == D_T:
                        return e[2], e[1], e[3], e[5]
            else:
                for e in data_r:
                    if e[0] == D_T:
                        return e[2], e[1], e[3], e[5]
        else:
            data_l, data_f = figure6b()
            if ty == "follower":
                for e in data_f:
                    if e[0] == exp:
                        return e[2], e[1], e[3]["99"], e[4]
            else:
                for e in data_l:
                    if e[0] == exp:
                        return e[2], e[1], e[3]["99"], e[4]
    elif protocol == "ref-copilot": # ref-copilot
        if exp == 0:
            data_l = figure_ref_copilot_no_slow()
            e = data_l[0]
            return e[2], e[1], e[3][98], e[4]
        else:
            data_l, data_f = figure_ref_copilot()
            if ty == "follower":
                for e in data_f:
                    if e[0] == exp:
                        return e[2], e[1], e[3][98], e[4]
            else:
                for e in data_l:
                    if e[0] == exp:
                        return e[2], e[1], e[3][98], e[4]
    else: # etcd
        data_3, data_5 = figure_etcd()
        if rep == 3:
            for e in data_3:
                if e[0] == exp:
                    return e[2], e[1], e[3][98], e[4]
        else:
            for e in data_5:
                if e[0] == exp:
                    return e[2], e[1], e[3][98], e[4]
    return 0, 0, 0, 0

@lru_cache(maxsize=None)
def get_cdf_data(protocol, ty, exp, rep):
    latency = []
    if protocol == "raft":
        if exp==0: # no slowness
            data_3, data_5 = figure5a()
        else:
            data_3, data_5 = figure5b()

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
    elif protocol == "copilot": # copilot
        if exp==0:
            data_r = figure6a()
            data_l, data_f = data_r, data_r
        else:
            data_l, data_f = figure6b()
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
    elif protocol == "ref-copilot":
        if exp==0:
            data_l = figure_ref_copilot_no_slow()
            e = data_l[0]
            latency = {str(i):e[3][i]/1000 for i in range(99)}
        else:
            data_l, data_f = figure_ref_copilot()

            if ty == "follower":
                for e in data_f:
                    if e[0] == exp:
                        latency = {str(i): e[3][i]/1000 for i in range(99) if e[3][i]/1000 < 10}
            else:
                for e in data_l:
                    if e[0] == exp:
                        latency = {str(i):e[3][i]/1000 for i in range(99)}
    elif protocol == "etcd":
        data_3, data_5 = figure_etcd()
        if rep == 3:
            for e in data_3:
                if e[0] == exp:
                    latency = {str(i):e[3][i] for i in range(99)}
        else:
            for e in data_5:
                if e[0] == exp:
                    latency = {str(i):e[3][i] for i in range(99)}

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


def median(a):
    m=max(a)
    if m==0: return 0,0
    copy=[e for e in a] 
    for i in range(len(a)):
        if a[i]==0:
            a[i]=m
    v_i=0
    v=sorted(a)[len(a)//2]
    for i in range(len(copy)):
        if copy[i]==v:
            v_i=i
    # revert back to a
    for i in range(len(a)):
        a[i] = copy[i]
    return v,v_i

def var(a):
    if max(a) == 0: return 0
    # standard deviation
    # we don't count 0, and it should not be 0
    is_exist=False
    for e in a:
        if e == 0:
            is_exist=True
    if is_exist:
        print("Please note, there is 0 in ", a)
    mean=sum(a)/len(a)
    dev=[(x - mean) ** 2 for x in a]
    ret = sum(dev) / len(a)
    return ret ** 0.5

def convert_yml_json(name):
  with open(name, 'r') as file:
      configuration = yaml.safe_load(file)
  
  with open(name+".json", 'w+') as json_file:
      json.dump(configuration, json_file)

  return json.load(open(name+".json"))

def find_the_yml(folder_name):
    if not os.path.isdir(folder_name):
        return ""

    for filename in os.listdir(folder_name):
        f = os.path.join(folder_name, filename)
        if os.path.isfile(f):
            if ".yml" in filename and ".json" not in filename:
                return folder_name + "/" + filename
    return ""


@lru_cache(maxsize=None)
def get_cdf_etcd(filename: str):
	with open(filename, 'r') as f:
		seen = False
		cdf = []
		for line in f:
			if 'Requests/sec' in line:
				avg_tput = float(line.rstrip('\n').split(':\t')[1])
			if not seen and 'Latency distribution' in line:
				seen = True
			elif seen:
				try:
					lat = float(line.rstrip('\n').split(': ')[1])
					cdf.append(lat * 1000)
				except:
					pass

		med = cdf[49]
		return avg_tput, med, cdf


@lru_cache(maxsize=None)
def figure5a_etcd():
    data_3, data_5=[],[]
    # (client, 50th, tps)
    # 3 replicas
    BASE_ETCD="/home/xuhao/depfast-test/scripts/etcd/"
    conc=[int(e) for e in "8 16 32 48 64 80 96 128 160 192 224 256 320 384 512".split(" ") if e]
    for e in conc:
        filename=BASE_ETCD+"log_3/experiments-"+str(e)+"/testname.txt"
        if os.path.exists(filename):
            avg_tput, med, cdf = get_cdf_etcd(filename)
            data_3.append([e, med, avg_tput])
        else:
            data_3.append([e, 0, 0])
    
    for e in conc:
        filename=BASE_ETCD+"log_5/experiments-"+str(e)+"/testname.txt"
        if os.path.exists(filename):
            avg_tput, med, cdf = get_cdf_etcd(filename)
            data_5.append([e, med, avg_tput])
        else:
            data_5.append([e, 0, 0])
    return data_3, data_5

@lru_cache(maxsize=None)
def figure5a():
    data_3_all, data_5_all=[],[]
    for i in range(FIGURE5a_TARIALS):
        data_3_t, data_5_t = figure5aTrail(i+1)
        data_3_all.append(data_3_t)
        data_5_all.append(data_5_t)

    data_3=data_3_all[0]
    data_5=data_5_all[0]
    H=len(data_3)

    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_99th=[]
        tmp_all=[]
        for j in range(FIGURE5a_TARIALS):
            tmp_50th.append(float(data_3_all[j][i][1]))
            tmp_tps.append(float(data_3_all[j][i][2]))
            tmp_99th.append(float(data_3_all[j][i][3]))
            tmp_all.append(data_3_all[j][i][4])
        data_3[i][2],v_i = median(tmp_tps)
        data_3[i][1] = tmp_50th[v_i]
        data_3[i][3] = tmp_99th[v_i]
        data_3[i][4] = tmp_all[v_i]
        data_3[i][5] = var(tmp_tps)

    H=len(data_5)
    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_99th=[]
        tmp_all=[]
        for j in range(FIGURE5a_TARIALS):
            tmp_50th.append(float(data_5_all[j][i][1]))
            tmp_tps.append(float(data_5_all[j][i][2]))
            tmp_99th.append(float(data_5_all[j][i][3]))
            tmp_all.append(data_5_all[j][i][4])
        data_5[i][2],v_i = median(tmp_tps)
        data_5[i][1] = tmp_50th[v_i]
        data_5[i][3] = tmp_99th[v_i]
        data_5[i][4] = tmp_all[v_i]
        data_5[i][5] = var(tmp_tps)
    return data_3, data_5

@lru_cache(maxsize=None)
def figure5aTrail(t):
    # 3 replicas
    if is_rw(): # rw
        conc=[20, 40, 60, 80, 100, 130, 160, 190, 200, 220, 260, 300, 340, 380, 420, 460, 500, 540, 580]
    else:  # tpca
        conc=[20, 40, 60, 80, 100, 130, 160, 190, 200, 220, 260, 300, 340, 380, 420]
    data_3=[] # (currency, 50th, tps, 99th, all_latency, tps-var)
    for i in conc:
        folder=BASE+"figure5a_"+str(t)+"/results_3_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_3.append([i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"]["99"], data[SK]["all_latency"], 0])
        else:
            data_3.append([i, 0, 0, 0, {str(i):0 for i in range(100)}, 0])
            print("No yml found in " + folder)
    
    # 5 replicas
    data_5=[]
    for i in conc:
        folder=BASE+"figure5a_"+str(t)+"/results_5_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_5.append([i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"]["99"], data[SK]["all_latency"], 0])
        else:
            data_5.append([i, 0, 0, 0, {str(i):0 for i in range(100)}, 0])
            print("No yml found in " + folder)
    return data_3, data_5

@lru_cache(maxsize=None)
def figure5b():
    data_3_all, data_5_all=[],[]
    for i in range(FIGURE5b_TARIALS):
        data_3_t, data_5_t = figure5bTrail(i+1)
        data_3_all.append(data_3_t)
        data_5_all.append(data_5_t)

    data_3=data_3_all[0]
    data_5=data_5_all[0]
    H=len(data_3)

    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_all=[]
        for j in range(FIGURE5b_TARIALS):
            tmp_50th.append(float(data_3_all[j][i][1]))
            tmp_tps.append(float(data_3_all[j][i][2]))
            tmp_all.append(data_3_all[j][i][3])
        data_3[i][2],v_i = median(tmp_tps)
        data_3[i][1] = tmp_50th[v_i]
        data_3[i][3] = tmp_all[v_i] 
        data_3[i][4] = var(tmp_tps)

    H=len(data_5)
    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_all=[]
        for j in range(FIGURE5b_TARIALS):
            tmp_50th.append(float(data_5_all[j][i][1]))
            tmp_tps.append(float(data_5_all[j][i][2]))
            tmp_all.append(data_5_all[j][i][3])
        data_5[i][2],v_i = median(tmp_tps)
        data_5[i][1] = tmp_50th[v_i]
        data_5[i][3] = tmp_all[v_i]
        data_5[i][4] = var(tmp_tps)
    return data_3, data_5

@lru_cache(maxsize=None)
def figure5bTrail(t):
    # 3 replicas
    exp=[1, 2, 3, 4, 5, 6]
    data_3=[] # (exp, 50th, tps, all_latency, tps-var)
    for i in exp:
        folder=BASE+"/figure5b_"+str(t)+"/results_3_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_3.append([i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"], 0])
        else:
            data_3.append([i, 0, 0, {str(i):0 for i in range(100)}, 0])
            print("No yml found in " + folder)
    
    # 5 replicas
    data_5=[]
    for i in exp:
        folder=BASE+"/figure5b_"+str(t)+"/results_5_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_5.append([i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"], 0])
        else:
            data_5.append([i, 0, 0, {str(i):0 for i in range(100)}, 0])
            print("No yml found in " + folder)

    return data_3, data_5


@lru_cache(maxsize=None)
def figure6a():
    data_r_all = []
    for i in range(FIGURE6a_TARIALS):
        data_r_t = figure6aTrail(i+1)
        data_r_all.append(data_r_t)

    data_r=data_r_all[0]
    H=len(data_r)

    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_99th=[]
        tmp_all=[]
        for j in range(FIGURE6a_TARIALS):
            tmp_50th.append(float(data_r_all[j][i][1]))
            tmp_tps.append(float(data_r_all[j][i][2]))
            tmp_99th.append(float(data_r_all[j][i][3]))
            tmp_all.append(data_r_all[j][i][4])
        data_r[i][2],v_i = median(tmp_tps)
        data_r[i][1] = tmp_50th[v_i]
        data_r[i][3] = tmp_99th[v_i]
        data_r[i][4] = tmp_all[v_i]
        data_r[i][5] = var(tmp_tps)

    return data_r

@lru_cache(maxsize=None)
def figure6aTrail(t):
    if is_rw():
        conc=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    else:
        conc=[1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
    data_r=[] # (currency, 50th, tps, 99th, all_latency, tps-var)
    for i in conc:
        folder=BASE+"/figure6a_"+str(t)+"/results_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_r.append([i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"]["99"], data[SK]["all_latency"], 0])
        else:
            data_r.append([i, 0, 0, 0, {str(i):0 for i in range(100)}, 0])
            print("No yml found in " + folder)
    
    return data_r

@lru_cache(maxsize=None)
def figure6b():
    data_l_all, data_f_all = [],[]
    for i in range(FIGURE6b_TARIALS):
        data_l_t, data_f_t = figure6bTrail(i+1)
        data_l_all.append(data_l_t)
        data_f_all.append(data_f_t)

    data_l=data_l_all[0]
    data_f=data_f_all[0]
    H=len(data_l)

    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_all=[]
        for j in range(FIGURE6b_TARIALS):
            tmp_50th.append(float(data_l_all[j][i][1]))
            tmp_tps.append(float(data_l_all[j][i][2]))
            tmp_all.append(data_l_all[j][i][3])
        data_l[i][2],v_i = median(tmp_tps)
        data_l[i][1] = tmp_50th[v_i]
        data_l[i][3] = tmp_all[v_i]
        data_l[i][4] = var(tmp_tps)

    H=len(data_f)
    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_all=[]
        for j in range(FIGURE6b_TARIALS):
            tmp_50th.append(float(data_f_all[j][i][1]))
            tmp_tps.append(float(data_f_all[j][i][2]))
            tmp_all.append(data_f_all[j][i][3])
        data_f[i][2],v_i = median(tmp_tps)
        data_f[i][1] = tmp_50th[v_i]
        data_f[i][3] = tmp_all[v_i]
        data_f[i][4] = var(tmp_tps)

    return data_l, data_f

@lru_cache(maxsize=None)
def figure6bTrail(t):
    # leader
    exp=[1, 2, 5, 6]
    data_l=[] # (exp, 50th, tps, all_latency, tps-var)
    for i in exp:
        folder=BASE+"/figure6b_"+str(t)+"/results_leader_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_l.append([i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"], 0])
        else:
            data_l.append([i, 0, 0, {str(i):0 for i in range(100)}, 0])
            print("No yml found in " + folder)
    
    # follower
    data_f=[]
    for i in exp:
        folder=BASE+"/figure6b_"+str(t)+"/results_follower_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_f.append([i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"], 0])
        else:
            data_f.append([i, 0, 0, {str(i):0 for i in range(100)}, 0])
            print("No yml found in " + folder)

    return data_l, data_f


@lru_cache(maxsize=None)
def figure6a_copilot():  # 1 trail is enough
    # backup: 0531_morning_4to64_4-16cpu_ref.copilot
    BASE_REF_CO="/home/xuhao/copilot/log"  # for ref.copilot
    clients=[int(e) for e in "4 8 12 16 20 36 40 48 56 60 64".split(" ")]
    data=[] # (# of clients, tps, all_latency)
    for c in clients:
        # # of clients
        items = [c]
        
        # get the filename
        filename=BASE_REF_CO+"/outputdir-"+str(c)+".log"
        exp_name=""
        if os.path.exists(filename):
            exp_name=[e for e in readtxt(filename)[0].split("/") if e][-1]

        # tps
        filename=BASE_REF_CO+"/experiments-"+str(c)+"/"+exp_name+"/tput.txt"
        if os.path.exists(filename):
            items.append(float(readtxt(filename)[0]))
        else:
            items.append(0)

        # all_latency
        filename=BASE_REF_CO+"/experiments-"+str(c)+"/"+exp_name+"/tputlat.txt"
        all_latency = []
        if os.path.exists(filename):
            values = readtxt(filename)[0].split("\t")
            items.append([float(values[i+1]) for i in range(99)])
        else:
            items.append([0 for i in range(99)])

        data.append(items)    
    return data


@lru_cache(maxsize=None)
def figure_ref_copilot():
    trails=3
    data_l_all, data_f_all = [],[]
    for i in range(trails):
        data_l_t, data_f_t = figure_ref_copilot_trail(i+1)
        data_l_all.append(data_l_t)
        data_f_all.append(data_f_t)

    data_l=data_l_all[0]
    data_f=data_f_all[0]
    H=len(data_l)

    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_all=[]
        for j in range(trails):
            tmp_50th.append(float(data_l_all[j][i][1]))
            tmp_tps.append(float(data_l_all[j][i][2]))
            tmp_all.append(data_l_all[j][i][3])
        data_l[i][2],v_i = median(tmp_tps)
        data_l[i][1] = tmp_50th[v_i]
        data_l[i][3] = tmp_all[v_i]
        data_l[i][4] = var(tmp_tps)

    H=len(data_f)
    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_all=[]
        for j in range(trails):
            tmp_50th.append(float(data_f_all[j][i][1]))
            tmp_tps.append(float(data_f_all[j][i][2]))
            tmp_all.append(data_f_all[j][i][3])
        data_f[i][2],v_i = median(tmp_tps)
        data_f[i][1] = tmp_50th[v_i]
        data_f[i][3] = tmp_all[v_i]
        data_f[i][4] = var(tmp_tps)

    return data_l, data_f

@lru_cache(maxsize=None)
def figure_ref_copilot_trail(t):
    BASE_REF_CO="/home/xuhao/copilot/"

    # leader
    exp=[1, 2, 5, 6]
    data_l=[] # (exp, 50th, tps, all_latency, tps-var)
    for i in exp:
        filename=BASE_REF_CO+"/log_exp_"+str(t)+"/outputdir_leader_"+str(i)+".log"
        exp_name=""
        if os.path.exists(filename):
            exp_name=[e for e in readtxt(filename)[0].split("/") if e][-1]

        filename=BASE_REF_CO+"/log_exp_"+str(t)+"/experiments_leader_"+str(i)+"/"+exp_name+"/tputlat.txt"
        items=[]
        if os.path.exists(filename):
            values = readtxt(filename)[0].split("\t")
            
            if float(values[0]) == 0:
                items.append(i)
                items.append(0)
                items.append(0)
                items.append([0 for i in range(99)])
                items.append(0)
            else:
                items.append(i)
                items.append(float(values[50]))
                items.append(float(values[0]))
                items.append([float(values[i+1]) for i in range(99)])
                items.append(0)
        else:
            items.append(i)
            items.append(0)
            items.append(0)
            items.append([0 for i in range(99)])
            items.append(0)
        data_l.append(items)
    
    # follower
    data_f=[]
    for i in exp:
        filename=BASE_REF_CO+"/log_exp_"+str(t)+"/outputdir_follower_"+str(i)+".log"
        exp_name=""
        if os.path.exists(filename):
            exp_name=[e for e in readtxt(filename)[0].split("/") if e][-1]

        filename=BASE_REF_CO+"/log_exp_"+str(t)+"/experiments_follower_"+str(i)+"/"+exp_name+"/tputlat.txt"
        items=[]
        if os.path.exists(filename) and i != 1:
            values = readtxt(filename)[0].split("\t")
            
            if float(values[0]) == 0:
                items.append(i)
                items.append(0)
                items.append(0)
                items.append([0 for i in range(99)])
                items.append(0)
            else:
                items.append(i)
                items.append(float(values[50]))
                items.append(float(values[0]))
                items.append([float(values[i+1]) for i in range(99)])
                items.append(0)
        else:
            items.append(i)
            items.append(0)
            items.append(0)
            items.append([0 for i in range(99)])
            items.append(0)
        data_f.append(items)

    return data_l, data_f

@lru_cache(maxsize=None)
def figure_ref_copilot_no_slow():
    trails=3
    data_l_all = []
    for i in range(trails):
        data_l_t = figure_ref_copilot_no_slow_trail(i+1)
        data_l_all.append(data_l_t)

    data_l=data_l_all[0]
    H=len(data_l)

    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_all=[]
        for j in range(trails):
            tmp_50th.append(float(data_l_all[j][i][1]))
            tmp_tps.append(float(data_l_all[j][i][2]))
            tmp_all.append(data_l_all[j][i][3])
        data_l[i][2],v_i = median(tmp_tps)
        data_l[i][1] = tmp_50th[v_i]
        data_l[i][3] = tmp_all[v_i]
        data_l[i][4] = var(tmp_tps)

    return data_l

@lru_cache(maxsize=None)
def figure_ref_copilot_no_slow_trail(t):
    BASE_REF_CO="/home/xuhao/copilot/"

    data_l=[] # (exp, 50th, tps, all_latency, tps-var)
    filename=BASE_REF_CO+"/log_no__"+str(t)+"/outputdir.log"
    exp_name=""
    if os.path.exists(filename):
        exp_name=[e for e in readtxt(filename)[0].split("/") if e][-1]

    filename=BASE_REF_CO+"/log_no__"+str(t)+"/experiments/"+exp_name+"/tputlat.txt"
    items=[]
    if os.path.exists(filename):
        values = readtxt(filename)[0].split("\t")
            
        if float(values[0]) == 0:
            items.append(0)
            items.append(0)
            items.append(0)
            items.append([0 for i in range(99)])
            items.append(0)
        else:
            items.append(0)
            items.append(float(values[50]))
            items.append(float(values[0]))
            items.append([float(values[i+1]) for i in range(99)])
            items.append(0)
    else:
        items.append(0)
        items.append(0)
        items.append(0)
        items.append([0 for i in range(99)])
        items.append(0)
        
    data_l.append(items)

    return data_l

@lru_cache(maxsize=None)
def figure_etcd():
    trails=3
    data_3_all, data_5_all = [],[]
    for i in range(trails):
        data_3_t, data_5_t = figure_etcd_trail(i+1)
        data_3_all.append(data_3_t)
        data_5_all.append(data_5_t)

    data_3=data_3_all[0]
    data_5=data_5_all[0]
    H=len(data_3)

    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_all=[]
        for j in range(trails):
            tmp_50th.append(float(data_3_all[j][i][1]))
            tmp_tps.append(float(data_3_all[j][i][2]))
            tmp_all.append(data_3_all[j][i][3])
        data_3[i][2],v_i = median(tmp_tps)
        data_3[i][1] = tmp_50th[v_i]
        data_3[i][3] = tmp_all[v_i]
        data_3[i][4] = var(tmp_tps)

    H=len(data_5)
    for i in range(H):
        tmp_50th=[]
        tmp_tps=[]
        tmp_all=[]
        for j in range(trails):
            tmp_50th.append(float(data_5_all[j][i][1]))
            tmp_tps.append(float(data_5_all[j][i][2]))
            tmp_all.append(data_5_all[j][i][3])
        data_5[i][2],v_i = median(tmp_tps)
        data_5[i][1] = tmp_50th[v_i]
        data_5[i][3] = tmp_all[v_i]
        data_5[i][4] = var(tmp_tps)

    return data_3, data_5


@lru_cache(maxsize=None)
def figure_etcd_trail(t):
    BASE_ETCD="/home/xuhao/depfast-test/scripts/etcd/"

    # leader
    exp=[0, 1, 2, 3, 4, 5, 6]
    data_3=[] # (exp, 50th, tps, all_latency, tps-var)
    data_5=[]
    for e in exp:
        filename=BASE_ETCD+"log_3_exp_"+str(t)+"/experiments_follower_"+str(e)+"/test3name.txt"
        if os.path.exists(filename):
            avg_tput, med, cdf = get_cdf_etcd(filename)
            data_3.append([e, med, avg_tput, cdf, 0])
        else:
            print("file not found: ", filename)
            data_3.append([e, 0, 0, [0]*99, 0])

        filename=BASE_ETCD+"log_5_exp_"+str(t)+"/experiments_follower_"+str(e)+"/test5name.txt"
        if os.path.exists(filename):
            avg_tput, med, cdf = get_cdf_etcd(filename)
            data_5.append([e, med, avg_tput, cdf, 0])
        else:
            print("file not found: ", filename)
            data_5.append([e, 0, 0, [0]*99, 0])

    return data_3, data_5

if __name__ == "__main__":
    #print(figure5a())
    #print(figure5b())
    #print(figure6a())
    #print(figure6b())
    #print(figure6a_copilot())
    #print(figure_ref_copilot())
    #print(figure_ref_copilot_no_slow())

    #print(figure5a_etcd())
    print(figure_etcd())
    
    print("")
