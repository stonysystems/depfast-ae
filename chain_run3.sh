# python3 waf configure build -J

t=10
msgsize=0
n_conc=$1
slowness=$2
# sudo apt install cpulimit
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
ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P c01 > c01.log 2>&1 &" &
ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P c02 > c02.log 2>&1 &" &
# ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P c03 > c03.log 2>&1 &" &
# ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P c04 > c04.log 2>&1 &" &
# ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P c05 > c05.log 2>&1 &" &
# ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P c06 > c06.log 2>&1 &" &
# ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P c07 > c07.log 2>&1 &" &
# ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P c08 > c08.log 2>&1 &" &
# ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -f config/concurrent_$n_conc.yml -f config/chainrpc/msgsize_$msgsize.yml -d $t -P c09 > c09.log 2>&1 &" &

sleep 20

ssh 192.168.1.102 "pkill deptran_server"
ssh 192.168.1.103 "pkill deptran_server"
ssh 192.168.1.103 "pkill cpulimit"
ssh 192.168.1.104 "pkill deptran_server"