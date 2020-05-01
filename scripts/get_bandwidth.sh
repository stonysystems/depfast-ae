#!/bin/bash
set -e
# set -x

nargs=3
duration=30
output_egress="~/egress_bandwidth"
output_ingress="~/ingress_bandwidth"
if [ "$#" -lt "$nargs" ]; then
	echo "Input 3 internal IPs of server address"
	exit 1
fi

echo "Starting iperf server on $1"
ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'nohup iperf3 -s > /dev/null 2>&1 &'"
echo "Starting iperf server on $2"
ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'nohup iperf3 -s > /dev/null 2>&1 &'"
echo "Starting iperf server on $3"
ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'nohup iperf3 -s > /dev/null 2>&1 &'"

echo "--------------------------------------------------------------------------"
echo "Testing for egress bandwidth on $1"
echo "Running iperf client on $1"
ssh -o StrictHostKeyChecking=no "$1" "sh -c 'nohup iperf3 -c "$2" -i 1 -t "$duration" -f gbits > /tmp/temp_bandwidth.txt'"
#ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat /tmp/temp_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_egress ; rm /tmp/temp_bandwidth.txt'"
ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat /tmp/temp_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_egress'"

bw1=$(ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat $output_egress'")
echo "Egress Bandwidth of $1=$bw1"
echo "Testing for ingress bandwidth on $1"
ssh -o StrictHostKeyChecking=no "$1" "sh -c 'nohup iperf3 -c "$2" -i 1 -t "$duration" -R -f gbits > /tmp/temp_ingress_bandwidth.txt'"
#ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat /tmp/temp_ingress_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_ingress ; rm /tmp/temp_ingress_bandwidth.txt'"
ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat /tmp/temp_ingress_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_ingress'"
ingress1bw1=$(ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat $output_ingress'")
ingress1bw1num=$(ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat $output_ingress | grep -Eo '[0-9]+.[0-9]+''")

ssh -o StrictHostKeyChecking=no "$1" "sh -c 'nohup iperf3 -c "$3" -i 1 -t "$duration" -R -f gbits > /tmp/temp_ingress_bandwidth2.txt'"
ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat /tmp/temp_ingress_bandwidth2.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_ingress'"
ingress2bw1=$(ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat $output_ingress'")
ingress2bw1num=$(ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat $output_ingress | grep -Eo '[0-9]+.[0-9]+''")

res1=$(echo "$ingress1bw1num $ingress2bw1num" | awk '{print ($1 > $2)}')
if [ "$res1" == 0 ]; then 
	ssh -o StrictHostKeyChecking=no "$1" "sh -c 'echo $ingress2bw1 > $output_ingress'"
	echo "Ingress Bandwidth of $1=$ingress2bw1"
else
	ssh -o StrictHostKeyChecking=no "$1" "sh -c 'echo $ingress1bw1 > $output_ingress'"
	echo "Ingress Bandwidth of $1=$ingress1bw1"
fi

echo "--------------------------------------------------------------------------"
echo "Testing for egress bandwidth on $2"
echo "Running iperf client on $2"
ssh -o StrictHostKeyChecking=no "$2" "sh -c 'nohup iperf3 -c "$1" -i 1 -t "$duration" -f gbits > /tmp/temp_bandwidth.txt'"
#ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat /tmp/temp_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_egress ; rm /tmp/temp_bandwidth.txt'"
ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'cat /tmp/temp_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_egress'"

bw2=$(ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'cat $output_egress'")
echo "Egress Bandwidth of $2=$bw2"
echo "Testing for ingress bandwidth on $2"
ssh -o StrictHostKeyChecking=no "$2" "sh -c 'nohup iperf3 -c "$1" -i 1 -t "$duration" -R -f gbits > /tmp/temp_ingress_bandwidth.txt'"
#ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat /tmp/temp_ingress_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_ingress ; rm /tmp/temp_ingress_bandwidth.txt'"
ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'cat /tmp/temp_ingress_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_ingress'"
ingress1bw2=$(ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'cat $output_ingress'")
ingress1bw2num=$(ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'cat $output_ingress | grep -Eo '[0-9]+.[0-9]+''")

ssh -o StrictHostKeyChecking=no "$2" "sh -c 'nohup iperf3 -c "$3" -i 1 -t "$duration" -R -f gbits > /tmp/temp_ingress_bandwidth2.txt'"
ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'cat /tmp/temp_ingress_bandwidth2.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_ingress'"
ingress2bw2=$(ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'cat $output_ingress'")
ingress2bw2num=$(ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'cat $output_ingress | grep -Eo '[0-9]+.[0-9]+''")

res2=$(echo "$ingress1bw2num $ingress2bw2num" | awk '{print ($1 > $2)}')
if [ "$res2" == 0 ]; then 
	ssh -o StrictHostKeyChecking=no "$2" "sh -c 'echo $ingress2bw2 > $output_ingress'"
	echo "Ingress Bandwidth of $2=$ingress2bw2"
else
	ssh -o StrictHostKeyChecking=no "$2" "sh -c 'echo $ingress1bw2 > $output_ingress'"
	echo "Ingress Bandwidth of $2=$ingress1bw2"
fi


echo "--------------------------------------------------------------------------"
echo "Testing for egress bandwidth on $3"
echo "Running iperf client on $3"
ssh -o StrictHostKeyChecking=no "$3" "sh -c 'nohup iperf3 -c "$1" -i 1 -t "$duration" -f gbits > /tmp/temp_bandwidth.txt'"
#ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat /tmp/temp_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_egress ; rm /tmp/temp_bandwidth.txt'"
ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'cat /tmp/temp_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_egress'"

bw3=$(ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'cat $output_egress'")
echo "Egress Bandwidth of $3=$bw3"
echo "Testing for ingress bandwidth on $3"
ssh -o StrictHostKeyChecking=no "$3" "sh -c 'nohup iperf3 -c "$1" -i 1 -t "$duration" -R -f gbits > /tmp/temp_ingress_bandwidth.txt'"
#ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'cat /tmp/temp_ingress_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_ingress ; rm /tmp/temp_ingress_bandwidth.txt'"
ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'cat /tmp/temp_ingress_bandwidth.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_ingress'"
ingress1bw3=$(ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'cat $output_ingress'")
ingress1bw3num=$(ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'cat $output_ingress | grep -Eo '[0-9]+.[0-9]+''")

ssh -o StrictHostKeyChecking=no "$3" "sh -c 'nohup iperf3 -c "$2" -i 1 -t "$duration" -R -f gbits > /tmp/temp_ingress_bandwidth2.txt'"
ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'cat /tmp/temp_ingress_bandwidth2.txt  | grep -Eo '\''[0-9]+.[0-9]+ Gbits/sec'\'' | sort -V -r | head -n1 > $output_ingress'"
ingress2bw3=$(ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'cat $output_ingress'")
ingress2bw3num=$(ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'cat $output_ingress | grep -Eo '[0-9]+.[0-9]+''")

res3=$(echo "$ingress1bw3num $ingress2bw3num" | awk '{print ($1 > $2)}')
if [ "$res3" == 0 ]; then 
	ssh -o StrictHostKeyChecking=no "$2" "sh -c 'echo $ingress2bw3 > $output_ingress'"
	echo "Ingress Bandwidth of $2=$ingress2bw3"
else
	ssh -o StrictHostKeyChecking=no "$2" "sh -c 'echo $ingress1bw3 > $output_ingress'"
	echo "Ingress Bandwidth of $2=$ingress1bw3"
fi

# Cleanup
echo "Cleaning up residual files"
ssh -o StrictHostKeyChecking=no  "$1" "sh -c 'pkill iperf ; rm /tmp/temp_ingress_*'"
ssh -o StrictHostKeyChecking=no  "$2" "sh -c 'pkill iperf ; rm /tmp/temp_ingress_*'"
ssh -o StrictHostKeyChecking=no  "$3" "sh -c 'pkill iperf ; rm /tmp/temp_ingress_*'"
