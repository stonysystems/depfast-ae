# python3 waf configure build -J
# awk '/Throughput:/ {match($0, /Throughput: ([0-9.]+)/, a); sum += a[1]} END {print sum}' *.log
# cat *.log | grep -Eo "Throughput: [0-9.]+"

# Runtime
t=30
# Dummy message size
msgsize=0
# Concurrent per client
n_conc=$1
# If enable slowness on follower-1 (sudo apt install cpulimit)
slowness=$2
# Number of client processes
nclients=20

leader=192.168.1.103
p1=192.168.1.102
p2=192.168.1.104
p3=192.168.1.102
p4=192.168.1.102

cmd_="ulimit -n 10000"

rm -f c*.log
ssh $leader "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_7.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P localhost > localhost.log 2>&1 &" &
sleep 0.4
ssh $p1 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_7.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P p1 > p1.log 2>&1 &" &
sleep 0.4
ssh $p2 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_7.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P p2 > p2.log 2>&1 &" &
sleep 0.4
ssh $p3 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_7.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P p3 > p3.log 2>&1 &" &
sleep 0.4
ssh $p4 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_7.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P p4 > p4.log 2>&1 &" &
sleep 0.4
ssh $p4 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_7.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P p5 > p5.log 2>&1 &" &
sleep 0.4
ssh $p4 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_7.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P p6 > p6.log 2>&1 &" &

sleep 5

# Start clients
clients=( c01 c02 c03 c04 c05 c06 c07 c08 c09 c10 c11 c12 c13 c14 c15 c16 c17 c18 c19 c20 )
clients=("${clients[@]:0:nclients}")
for c in "${clients[@]}"
do
  echo "start clients: $c..."
  ssh $leader "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_5.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P $c > $c.log 2>&1 &" &
  sleep 0.1
done

sleep_time=$((t + 10))
sleep $sleep_time

./kill.sh