#!/bin/bash

set -ex

secondaryip=$1
secondarypid=$2

 ssh -i ~/.ssh/id_rsa "$secondaryip" "sh -c 'nohup taskset -ac 0 ~/deadloop > /dev/null 2>&1 &'"
 deadlooppid=$(ssh -i ~/.ssh/id_rsa "$secondaryip" "sh -c 'pgrep deadloop'")
 ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo mkdir /sys/fs/cgroup/cpu/cpulow /sys/fs/cgroup/cpu/cpuhigh'"
 ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo echo 64 > /sys/fs/cgroup/cpu/cpulow/cpu.shares'"
 ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo echo $deadlooppid > /sys/fs/cgroup/cpu/cpuhigh/cgroup.procs'"
 ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo echo $secondarypid > /sys/fs/cgroup/cpu/cpulow/cgroup.procs'"
