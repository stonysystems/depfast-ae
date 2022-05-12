
# DepFast


## Getting Started Instructions (with Ubuntu 16.04 or newer)

Install dependencies:

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

Get source code:
```
git clone --recursive https://github.com/WolfDOS/depfast.git
```

Build:

```
git checkout atc_ae
python3 waf configure build 
```

Test run:
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

## More
Check out the doc directory to find more about how to build the system on older or newer distros, how to run the system in a distributed setup, and how to generate figures in the paper, etc.

