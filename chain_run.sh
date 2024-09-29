# python3 waf configure build -J

./build/deptran_server -f config/monolithic_chainrpc.yml -d 10 -P localhost > localhost.log 2>&1 &
sleep 0.4
./build/deptran_server -f config/monolithic_chainrpc.yml -d 10 -P p1 > p1.log 2>&1 &
sleep 0.4
./build/deptran_server -f config/monolithic_chainrpc.yml -d 10 -P p2 > p2.log 2>&1 &
sleep 2
./build/deptran_server -f config/monolithic_chainrpc.yml -d 10 -P c01 > c01.log 2>&1 &

sleep 12

pkill deptran_server
