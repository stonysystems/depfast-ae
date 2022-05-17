
# DepFast

Welcome to the DepFast artifact for our ATC'22 submission.

*Building Fault-Tolerant Distributed Systems with DepFast*

## Run the experiment locally
In this section, you can set up DepFast locally for testing and verification, which is mainly for badges **Artifacts Available** and **Artifacts Evaluated - Functional**.

### 1. create a Docker instance
We run all our codes on ubuntu 20.04 which mainly depends on several Linux libraries (i.e., boost, gcc and libyaml-cpp-dev). We provide a docker image with all required dependencies and source code for ease so you can run on any local machine supporting Docker.
```bash
# on any machine with Docker support
cd ~
git clone https://github.com/stonysystems/depfast-ae.git
git checkout atc_ae
cd ~/depfast-ae/docker
bash run.sh
```

### 2. run minimal working examples locally
You can start DepFast instance locally (using different processes to mimic actual distributed environment) to verify the functionability of the program inside the docker container now.

```bash
# enter the docker container (outside the container)
sudo docker exec -it ubuntu_atc2022 /bin/bash

# run 4 minimal experiments (inside the container): 
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
In this section, we will build a actual distributed environment to reproduce our results in the paper, which is mainly for badges **Results Reproduced**.

### 1. setup machines
To reproduce results in the paper, we need 
 - **obtain 4 machines**: 5 servers + 1 client running on Ubuntu 20.04
 - obtain the IP of 6 machines: `[server-1-ip]`, `[server-2-ip]`, `[server-3-ip]`, `[server-4-ip]`, `[server-5-ip]`, `[client-1-ip]`.
 - ensure that 6 machines can connect to each other via `ssh` and share the same username which means you can connect to any other machines on any machine through `ssh ip` directly without username required

### 2. setup environment on all machines
Let's assume
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

bash ./batch_op.sh scp
bash ./batch_op.sh dep

# compile
python3 waf configure build
```

### 3. run all experiments in the one-click script
We provide one-click runnable script to generate all results under folder ./results. It would take up to ~3 hours to run all experiments.
```bash
# run commands on the client-1-ip machine
cd ~/code/depfast
bash one-click.sh
```