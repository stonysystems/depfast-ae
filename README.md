
# DepFast


## Getting Started Instructions (with Ubuntu 16.04 or newer)

### Install dependencies:

```sh
sudo apt-get update
sudo apt-get install -y \
    git \
    pkg-config \
    build-essential \
    clang \
    cgroup-tools \
    libapr1-dev libaprutil1-dev \
    libboost-all-dev \
    libyaml-cpp-dev \
    libjemalloc-dev \
    python3-dev \
    python3-pip \
    python3-wheel \
    python3-setuptools \
    libgoogle-perftools-dev \
sudo wget https://github.com/mikefarah/yq/releases/download/v4.24.2/yq_linux_amd64 \
    -O /usr/bin/yq && sudo chmod +x /usr/bin/yq
sudo pip3 install -r requirements.txt
```

### Get source code:
```
git clone --recursive https://github.com/WolfDOS/depfast.git
```

### Build:

```
git checkout atc_ae
python3 waf configure build 
```

### Test run:
```
python3 test_run.py
```
If the test passed, the output should show result as `OK`, like this:
```
mode           site      bench     concurrent     result         time 
none_copilot   1c1s3r1p  rw        concurrent_1   OK             17.14s
```
This script tests DepFast in a single-node, single-process setting, where different nodes are simulated with different threads.

## Detailed Instructions

### Run single evaluation:
```sh
./start-exp.sh ${exp_name} ${N_concurrent} ${duration} ${exp} ${N_replica} ${slow_type} ${N_client} ${protocol}
```
The meaning of each parameter are as follow:
- `exp_name`(string): name of experiment
- `N_concurrent`(int): max number of outstanding requests each client can have
- `duration`(int): duration(s) of experiment
- `exp`(int): which slowness to inject
    - 0: no slowness
    - 1: slow CPU
    - 2: CPU contention
    - 3: slow disk
    - 4: disk contention
    - 5: slow network
    - 6: memory contention
- `N_replica`(int): number of replica
- `slow_type`(string): slowness injected to leader or follower, use `leader` or `follower`
- `N_client`(int): number of client
- `protocol`(string): consensus protocol to run, please use `fpga_raft` for Raft and `copilot` for Copilot

e.g. the following command
```sh
./start-exp.sh test 10 20 1 3 follower 10 copilot
```
means: run Copilot in a 3-replica setting using 10 clients, each client has 10 outstanding requests, cpu slowness is injected to the follower, runs for 20s

Key evaluation results will be written into `result${exp}_${N_replica}.csv` under the same folder, in the format of
```csv
exp name, average throughput, average latency, median latency, 99% latency
```


