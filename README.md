
# DepFast

Please refer to https://github.com/stonysystems/depfast-ae/blob/atc_ae/README.md for the most up-to-date README.

## Getting Started Instructions (with Ubuntu 20.04 or newer)

### Install dependencies:

```sh
sudo apt-get update
sudo apt-get install -y \
    git \
    wget \
    python2 \
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
    libgoogle-perftools-dev
sudo wget https://github.com/mikefarah/yq/releases/download/v4.24.2/yq_linux_amd64 \
    -O /usr/bin/yq && sudo chmod +x /usr/bin/yq
sudo pip3 install -r requirements.txt
```

### Get source code:
```
git clone --recursive https://github.com/stonysystems/depfast-ae.git depfast
```

### Build:

```
cd depfast
git checkout atc_ae
python3 waf configure build 
```

### Test run:
```
ulimit -n 10000
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

#### Set cluster IP

If you want to evaluate on a multi-node cluster, please first change the ip config file `config/host-nonlocal.yml`(for 3-replica) or `config/host-nonlocal-5.yml`(for 5-replica) to configure the ip of each node. Currently we only support 3-replica and 5-replica.

Here is an example config of a 3-replica cluster:
```yaml
# host-nonlocal.yml
host:
    host1: 10.0.0.13 # server 0
    host2: 10.0.0.14 # server 1
    host3: 10.0.0.15 # server 2
    host4: 10.0.0.37 # client
```
For an n-replica cluster, the first n lines are the ip addresses of the n servers, the remaining lines are the ip addresses of clients.

> ps: Please add the fingerprints of all ips in the config file into known_hosts by `ssh-keyscan -H ${ip_address} >> ~/.ssh/known_hosts`

#### Command line usage
To run a single evaluation, please use the `start-exp.sh` in the following way:
```sh
./start-exp.sh ${exp_name} ${duration} ${exp} ${N_replica} ${slow_type} ${N_client} ${N_concurrent} ${protocol} ${env}
```
The meaning of each parameter are as follow:
- `exp_name`(string): name of experiment
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
- `N_concurrent`(int): max number of outstanding requests each client can have
- `protocol`(string): consensus protocol to run, please use `fpga_raft` for Raft and `copilot` for Copilot
- `env`(string): run on single node or a distributed cluster, use `local` to run on a single node and `non-local` to run on a distributed cluster

e.g. the following command
```sh
./start-exp.sh test 20 1 3 follower 10 10 copilot non-local
```
means: run Copilot in a distributed 3-replica setting using 10 clients, each client has 10 outstanding requests, cpu slowness is injected to the follower, runs for 20s

> ps: The `start-exp.sh` can be run on any machine (the *control node*) that has access to all cluster nodes (typically we run it on the client node). Please make sure that DepFast is installed in the home folder of the same user as the control node (if user `alice` is running the scripts in `/home/alice/depfast` on the control node, then DepFast should be installed under `/home/alice/depfast` on all server and client nodes), since we don't specify the username and credential when we ssh into those nodes. It's recommended to have DepFast installed on one node and use NFS to mount it to all other nodes.

#### Results
Key evaluation results will be written into `result${exp}_${N_replica}.csv` under the same folder, in the format of
```csv
exp name, average throughput, average latency, median latency, 99% latency
```
Full results are saved in `log` folder in the format of a yaml file
