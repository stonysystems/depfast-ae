# python3 waf configure build -J

# Runtime
t=10
# Dummy message size
msgsize=0
# Concurrent per client
n_conc=$1
# If enable slowness on follower-1 (sudo apt install cpulimit)
slowness=$2
# Number of client processes
nclients=2

slowness_cmd=""
if [ "$slowness" == "slow" ]; then
    slowness_cmd="cpulimit --exe deptran_server --limit 20"
else
    slowness_cmd="echo ''"
fi
cmd_="ulimit -n 10000"

rm -f c*.log
ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P localhost > localhost.log 2>&1 &" &
sleep 0.4
ssh 192.168.1.103 "$slowness_cmd &" &
ssh 192.168.1.103 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P p1 > p1.log 2>&1 &" &
sleep 0.4
ssh 192.168.1.104 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P p2 > p2.log 2>&1 &" &
sleep 5

# Start clients
clients=( c01 c02 c03 c04 c05 c06 c07 c08 c09 )
clients=("${clients[@]:0:nclients}")
for c in "${clients[@]}"
do
  echo "start clients: $c..."
  ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P $c > $c.log 2>&1 &" &
done

sleep_time=$((t + 10))
sleep $sleep_time

ssh 192.168.1.102 "pkill deptran_server"
ssh 192.168.1.103 "pkill deptran_server"
ssh 192.168.1.103 "pkill cpulimit"
ssh 192.168.1.104 "pkill deptran_server"