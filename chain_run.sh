# python3 waf configure build -J

t=10
cmd_="ulimit -n 10000"

ssh 192.168.1.102  "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P localhost > localhost.log 2>&1 &" &
sleep 0.4
ssh 192.168.1.103  "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P p1 > p1.log 2>&1 &" &
sleep 0.4
ssh 192.168.1.103  "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P p2 > p2.log 2>&1 &" &
sleep 0.4
ssh 192.168.1.104  "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P p3 > p3.log 2>&1 &" &
sleep 0.4
ssh 192.168.1.104  "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P p4 > p4.log 2>&1 &" &
sleep 5
ssh 192.168.1.102  "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P c01 > c01.log 2>&1 &" &
ssh 192.168.1.102  "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P c02 > c02.log 2>&1 &" &

# ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -d $t -P localhost > localhost.log 2>&1 &" &
# sleep 0.4
# ssh 192.168.1.103 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -d $t -P p1 > p1.log 2>&1 &" &
# sleep 0.4
# ssh 192.168.1.104 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -d $t -P p2 > p2.log 2>&1 &" &
# sleep 5
# ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -d $t -P c01 > c01.log 2>&1 &" &
# ssh 192.168.1.102 "$cmd_; cd ~/depfast-ae && ./build/deptran_server -f config/monolithic_chainrpc.yml -d $t -P c02 > c02.log 2>&1 &" &

sleep 20

ssh 192.168.1.102 "pkill deptran_server"
ssh 192.168.1.103 "pkill deptran_server"
ssh 192.168.1.104 "pkill deptran_server"

