# python3 waf configure build -J

t=10

./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P localhost > localhost.log 2>&1 &
sleep 0.4
./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P p1 > p1.log 2>&1 &
sleep 0.4
./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P p2 > p2.log 2>&1 &
sleep 0.4
./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P p3 > p3.log 2>&1 &
sleep 0.4
./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P p4 > p4.log 2>&1 &
sleep 2
./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P c01 > c01.log 2>&1 &
./build/deptran_server -f config/monolithic_chainrpc_5.yml -d $t -P c01 > c02.log 2>&1 &

# ./build/deptran_server -f config/monolithic_chainrpc.yml -d $t -P localhost > localhost.log 2>&1 &
# sleep 0.4
# ./build/deptran_server -f config/monolithic_chainrpc.yml -d $t -P p1 > p1.log 2>&1 &
# sleep 0.4
# ./build/deptran_server -f config/monolithic_chainrpc.yml -d $t -P p2 > p2.log 2>&1 &
# sleep 2
# ./build/deptran_server -f config/monolithic_chainrpc.yml -d $t -P c01 > c01.log 2>&1 &
# ./build/deptran_server -f config/monolithic_chainrpc.yml -d $t -P c02 > c02.log 2>&1 &

sleep 15

pkill deptran_server
