import yaml
import json
import os

# the library for the data-processing
BASE="../"
SK="PAYMENT"
#SK="WRITE"

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

def figure5a():
    # 3 replicas
    conc=[20, 40, 60, 80, 100, 130, 160, 190, 200, 220, 260, 300, 340, 380, 420]
    data_3=[] # (currency, 50th, tps, 99th)
    for i in conc:
        folder=BASE+"figure5a/results_3_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_3.append((i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"]["99"]))
        else:
            print("No yml found in " + folder)
    
    # 5 replicas
    data_5=[]
    for i in conc:
        folder=BASE+"figure5a/results_5_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_5.append((i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"]["99"]))
        else:
            print("No yml found in " + folder)
    return data_3, data_5


def figure5b():
    # 3 replicas
    exp=[1, 2, 3, 4, 5, 6]
    data_3=[] # (exp, 50th, tps, all_latency)
    for i in exp:
        folder=BASE+"/figure5b/results_3_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_3.append((i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"]))
        else:
            print("No yml found in " + folder)
    
    # 5 replicas
    data_5=[]
    for i in exp:
        folder=BASE+"/figure5b/results_5_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_5.append((i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"]))
        else:
            print("No yml found in " + folder)

    return data_3, data_5


def figure6a():
    conc=[1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]
    data_r=[] # (currency, 50th, tps, 99th)
    for i in conc:
        folder=BASE+"/figure6a/results_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_r.append((i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"]["99"]))
        else:
            print("No yml found in " + folder)
    
    return data_r

def figure6b():
    # leader
    exp=[1, 2, 5, 6]
    data_l=[] # (exp, 50th, tps, all_latency)
    for i in exp:
        folder=BASE+"/figure6b/results_leader_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_l.append((i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"]))
        else:
            print("No yml found in " + folder)
    
    # follower
    data_f=[]
    for i in exp:
        folder=BASE+"/figure6b/results_follower_"+str(i)
        if find_the_yml(folder):
            data=convert_yml_json(find_the_yml(folder))
            data_f.append((i, data[SK]["all_latency"]["50"], data[SK]["tps"], data[SK]["all_latency"]))
        else:
            print("No yml found in " + folder)

    return data_l, data_f


if __name__ == "__main__":
    #print(figure5a())
    #print(figure5b())
    #print(figure6a())
    #print(figure6b())
    pass
