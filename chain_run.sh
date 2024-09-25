python3 waf configure build -J

./build/deptran_server -f config/monolithic_chainrpc.yml -d 30 -P localhost &
sleep 0.4
./build/deptran_server -f config/monolithic_chainrpc.yml -d 30 -P p1 &
sleep 0.4
./build/deptran_server -f config/monolithic_chainrpc.yml -d 30 -P p2 &
sleep 0.4
./build/deptran_server -f config/monolithic_chainrpc.yml -d 30 -P c01 &