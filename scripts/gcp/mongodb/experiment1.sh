#!/bin/bash

set -ex

secondaryip=$1
secondarypid=$2

ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo mkdir /sys/fs/cgroup/cpu/db'"
ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo echo 50000 > /sys/fs/cgroup/cpu/db/cpu.cfs_quota_us'"
ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo echo 1000000 > /sys/fs/cgroup/cpu/db/cpu.cfs_period_us'"
ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo echo $secondarypid > /sys/fs/cgroup/cpu/db/cgroup.procs'"
