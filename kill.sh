ssh 192.168.1.102 "ps aux  |  grep -i deptran |  awk '{print \$2}'  |  xargs kill -9"
ssh 192.168.1.103 "ps aux  |  grep -i deptran |  awk '{print \$2}'  |  xargs kill -9"
ssh 192.168.1.104 "ps aux  |  grep -i deptran |  awk '{print \$2}'  |  xargs kill -9"
ssh 192.168.1.102 "ps aux  |  grep -i cpulimit |  awk '{print \$2}'  |  xargs kill -9"
ssh 192.168.1.103 "ps aux  |  grep -i cpulimit |  awk '{print \$2}'  |  xargs kill -9"
ssh 192.168.1.104 "ps aux  |  grep -i cpulimit |  awk '{print \$2}'  |  xargs kill -9"
