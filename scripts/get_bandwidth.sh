#!/bin/sh
set -e
# set -x

nargs=3
output_file="~/bandwidth"
if [ "$#" -lt "$nargs" ]; then
	echo "Input 3 internal IPs of server address"
	exit 1
fi

echo "Running iperf server on $2"
ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'nohup iperf3 -s > /dev/null 2>&1 &'"

# Run client here
echo "Running iperf client on $1"
ssh -o StrictHostKeyChecking=no "$1" "sh -c 'nohup iperf3 -c "$2" -i 1 -t 30 > /tmp/temp_bandwidth.txt'"
ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat /tmp/temp_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ .bits/sec'\'' | sort -V -r | head -n1 > $output_file ; rm /tmp/temp_bandwidth.txt'"

bw1=$(ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat $output_file'")
echo "Bandwidth of $1=$bw1"
#rm /tmp/temp_bandwidth.txt

echo ""
echo "Running iperf server on $1"
ssh -o StrictHostKeyChecking=no "$1" "sh -c 'nohup iperf3 -s > /dev/null 2>&1 &'"

echo "Running iperf client on $2"
ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'nohup iperf3 -c "$1" -i 1 -t 30 > /tmp/temp_bandwidth.txt'"
ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'cat /tmp/temp_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ .bits/sec'\'' | sort -V -r | head -n1 > $output_file ; rm /tmp/temp_bandwidth.txt'"
bw2=$(ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'cat $output_file'")
echo "Bandwidth of $2=$bw2"

echo ""
echo "Running iperf client on $3"
# This uses the server running on $1
ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'nohup iperf3 -c "$1" -i 1 -t 30 > /tmp/temp_bandwidth.txt'"
ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'cat /tmp/temp_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ .bits/sec'\'' | sort -V -r | head -n1 > $output_file ; rm /tmp/temp_bandwidth.txt'"
bw3=$(ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'cat $output_file'")
echo "Bandwidth of $3=$bw3"

# Cleanup
echo "Cleaning up residual files"
ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'pkill iperf'"
ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'pkill iperf'"
