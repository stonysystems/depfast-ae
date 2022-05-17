#!/bin/bash

sudo ./cleanup.sh
sudo mkdir /db
echo y | sudo mkfs -t ext4 /dev/sdc
sudo mount /dev/sdc /db
sudo chmod o+w /db
touch /db/data.txt
sudo chmod o+w /db/data.txt # data file for raft

sudo dd if=/dev/zero of=/db/swapfile bs=1024 count=1048576
sudo chmod 600 /db/swapfile
sudo mkswap /db/swapfile 

