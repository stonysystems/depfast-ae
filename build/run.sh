rm -rf CMakeCache.txt CMakeFiles deptran_server hello_server hello_client
cmake ..
make deptran_server -j10 
make hello_server -j10
make hello_client -j10
