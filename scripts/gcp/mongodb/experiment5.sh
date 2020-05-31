#!/bin/bash

set -ex

ip=$1

ssh -i ~/.ssh/id_rsa "$ip" "sudo sh -c 'sudo /sbin/tc qdisc add dev ens4 root netem delay 400ms'"
