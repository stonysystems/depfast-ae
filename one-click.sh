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

setup () {
    bash ./batch_op.sh kill
}

experiment1() {
    python3 waf configure build
    bash ./batch_op.sh scp 
   
    ./start-exp.sh testname 60 0 3 follower 1 12
}

setup

experiment1
