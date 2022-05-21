
# DepFast

Welcome to the DepFast artifact for our ATC'22 submission.

*Building Fault-Tolerant Distributed Systems with DepFast*

## Run the experiment locally
<!-- https://github.com/ikatyang/emoji-cheat-sheet/blob/master/README.md -->
In this section, you can set up DepFast locally for testing and verification, which is mainly for badges :green_circle: `Artifacts Available` and :green_circle: `Artifacts Evaluated - Functional`. 

### 1. create a Docker instance (~30minutes)
We run all our codes on `ubuntu 20.04` which mainly depends on several Linux libraries (i.e., boost, gcc and libyaml-cpp-dev). We provide a docker image with all required dependencies and source code for ease so you can run on any local machine supporting Docker.
```bash
# on any machine with Docker support
cd ~
git clone https://github.com/stonysystems/depfast-ae.git
cd ~/depfast-ae
git checkout atc_ae
cd ~/depfast-ae/docker
sudo docker build -t ubuntu_atc2022 .
sudo docker run --name ubuntu_atc2022 -it ubuntu_atc2022
```

### 2. run minimal working examples locally (~5minutes)
You can start DepFast instance locally (using different processes to mimic actual distributed environment) to verify the functionability of the program inside the docker container now.

```bash
# enter the docker container - outside the container
sudo docker exec -it ubuntu_atc2022 /bin/bash

# run 4 minimal experiments - inside the container: 
python3 test_run.py
```

If the test passed, the output should show result as OK, like this:
```
mode           site      bench     concurrent     result 	 time
none_copilot   1c1s3r1p  rw        concurrent_1   OK     	 18.17s
none_copilot   1c1s3r1p  rw        concurrent_10  OK     	 18.17s
none_fpga_raft 1c1s3r1p  rw        concurrent_1   OK     	 18.12s
none_fpga_raft 1c1s3r1p  rw        concurrent_10  OK     	 18.17s
```

## Run the expriment in actual distributed environment
In this section, we will build a actual distributed environment to reproduce our results in the paper, which is mainly for badges :green_circle: `Results Reproduced`.

### 1. setup machines
To reproduce results in the paper, we need 
 - **obtain 6 machines**: 5 servers + 1 client running on Ubuntu `20.04`
 - obtain the IP of 6 machines: `[server-1-ip]`, `[server-2-ip]`, `[server-3-ip]`, `[server-4-ip]`, `[server-5-ip]`, `[client-1-ip]`. (*each machine should have at least 4 cpu cores*)
 - ensure that 6 machines can connect to each other via `ssh` and **share the same username** which means you can connect to any other machines on any machine through `ssh ip` directly without username required

### 2. setup environment on all machines
Let's assume we have 6 machines, 
- `[server-1-ip]` -> `10.0.0.13`
- `[server-2-ip]` -> `10.0.0.14`
- `[server-3-ip]` -> `10.0.0.15`
- `[server-4-ip]` -> `10.0.0.55`
- `[server-5-ip]` -> `10.0.0.58`
- `[client-1-ip]` -> `10.0.0.37`. 

Run all following commands on the `client-1-ip`.
```bash
mkdir -p ~/code
cd ~/code
git clone --recursive https://github.com/stonysystems/depfast-ae.git depfast
cd ~/code/depfast
git checkout atc_ae

# config IPs
./ip_config.sh 10.0.0.13 10.0.0.14 10.0.0.15 10.0.0.55 10.0.0.58 10.0.0.37 

# sync code to all servers
bash ./batch_op.sh scp
# install dependencies on all servers
bash ./batch_op.sh dep

# compile
python3 waf configure -J build
bash ./batch_op.sh scp
```

### 3. run all experiments in the one-click script (~8hours)
We provide one-click runnable script to generate all results. **We strongly recommend you run this script within the `tmux` in case the task is terminated unexpectedly.**
```bash
# run commands on the client-1-ip machine
cd ~/code/depfast
bash one-click.sh
```

Once everything is done, figures (`figure-5: depfast_raft.pdf` and `figure-6: depfast_copilot.pdf`) reported in the paper are generated under `./data_processing/imgs`.
```
# ls -lh ./data_processing/imgs
total 72K
-rw-r--r-- 1 wshen24 sudo-users 29K May 18 16:30 depfast_copilot.pdf
-rw-r--r-- 1 wshen24 sudo-users 38K May 18 16:29 depfast_raft.pdf
```
