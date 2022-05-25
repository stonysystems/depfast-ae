ps aux  |  grep -i depfast |  awk '{print $2}'  |  xargs sudo kill -9
ps aux  |  grep -i run.py |  awk '{print $2}'  |  xargs sudo kill -9
ps aux  |  grep -i run_exp1.py |  awk '{print $2}'  |  xargs sudo kill -9
ps aux  |  grep -i run_exp2.py  |  awk '{print $2}'  |  xargs sudo kill -9
ps aux  |  grep -i run_exp3.py  |  awk '{print $2}'  |  xargs sudo kill -9
ps aux  |  grep -i run_exp4.py  |  awk '{print $2}'  |  xargs sudo kill -9
ps aux  |  grep -i run_exp5.py  |  awk '{print $2}'  |  xargs sudo kill -9
ps aux  |  grep -i run_exp6.py  |  awk '{print $2}'  |  xargs sudo kill -9
ps aux  |  grep -i inf |  awk '{print $2}'  |  xargs sudo kill -9
sudo /sbin/tc qdisc del dev eth0 root
sleep 1
