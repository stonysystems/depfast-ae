s1=$1
s2=$2
s3=$3
s4=$4
s5=$5
c1=$6
echo $s1 > ./ips/ip_s1 
echo $s2 > ./ips/ip_s2 
echo $s3 > ./ips/ip_s3 
echo $s4 > ./ips/ip_s4 
echo $s5 > ./ips/ip_s5
echo $c1 > ./ips/ip_c1 
sed -i "s/host1: 10.0.0.13/host1: $s1/g" config/hosts-nonlocal.yml 
sed -i "s/host2: 10.0.0.14/host2: $s2/g" config/hosts-nonlocal.yml 
sed -i "s/host3: 10.0.0.15/host3: $s3/g" config/hosts-nonlocal.yml 
sed -i "s/host4: 10.0.0.37/host4: $c1/g" config/hosts-nonlocal.yml 

sed -i "s/host1: 10.0.0.13/host1: $s1/g" config/hosts-nonlocal-5.yml 
sed -i "s/host2: 10.0.0.14/host2: $s2/g" config/hosts-nonlocal-5.yml 
sed -i "s/host3: 10.0.0.15/host3: $s3/g" config/hosts-nonlocal-5.yml 
sed -i "s/host4: 10.0.0.55/host4: $s4/g" config/hosts-nonlocal-5.yml 
sed -i "s/host5: 10.0.0.58/host5: $s5/g" config/hosts-nonlocal-5.yml 
sed -i "s/host6: 10.0.0.37/host6: $c1/g" config/hosts-nonlocal-5.yml 
