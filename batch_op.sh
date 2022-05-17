repos="depfast"  # repos name, default
workdir="~/code"  # we default put our repos under the root

s1=$( cat ./ips/ip_s1 )
s2=$( cat ./ips/ip_s2 )
s3=$( cat ./ips/ip_s3 )
s4=$( cat ./ips/ip_s4 )
s5=$( cat ./ips/ip_s5 )
c1=$( cat ./ips/ip_c1 )
servers=(
  $s1
  $s2
  $s3
)

# local operation
cmd1=""
cmd2="cd $workdir ; sudo bash dep.sh; sudo pip3 install -r requirements.txt"
cmd3=""

if [ $1 == 'scp' ]; then
	:
elif [ $1 == 'dep' ]; then
    :
elif [ $1 == 'kill' ]; then
    :
else
  :
fi


# sync to others
cmd1="cd $workdir ; sudo rm -rf $repos; "
cmd2="cd $workdir ; sudo bash dep.sh; sudo pip3 install -r requirements.txt"
cmd3=""

for host in ${servers[@]}
do
  if [ $1 == 'scp' ]; then
    echo "scp to $host cmd: $cmd1"
    ssh $host "$cmd1" &
  elif [ $1 == 'dep' ]; then
    echo "scp to $host cmd: $cmd2"
    ssh $host "$cmd2" &
elif [ $1 == 'kill' ]; then
    :
  else
	  :
  fi
done

echo "Wait for jobs..."
FAIL=0

for job in `jobs -p`
do
    wait $job || let "FAIL+=1"
done

if [ "$FAIL" == "0" ];
then
    echo "YAY!"
else
    echo "FAIL! ($FAIL)"
fi

