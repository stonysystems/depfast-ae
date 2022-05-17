repos="depfast"  # repos name, default
workdir="~/code"  # we default put our repos under the root

s1=$( cat ./ips/ip_s1 )
s2=$( cat ./ips/ip_s2 )
s3=$( cat ./ips/ip_s3 )
s4=$( cat ./ips/ip_s4 )
s5=$( cat ./ips/ip_s5 )
c1=$( cat ./ips/ip_c1 )
servers=(
  $s1
  $s2
  $s3
)

ulimit -n 10000

# which slowness to injecta, 0-6
exp=( 0 1 2 3 4 5 6 )
# # of replicas 
replicas=( 3 5 )
# slow types: follower, leader
slow_type=( follower leader )
# number of client
N_client=1


setup () {
    bash ./batch_op.sh kill
}

# in figure5a
experiment1() {
    python3 waf configure build
    bash ./batch_op.sh init 
    bash ./batch_op.sh scp 
    mkdir ./figure5a
   
    for i in "${array[@]}"
    do
    	echo "$i"
    done
    ./start-exp.sh testname 60 0 3 follower 1 12 copilot nonlocal
}

setup

experiment1
