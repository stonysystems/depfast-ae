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
  $s4
  $s5
)

# local operation
cmd1="cd $workdir/$repos ; mkdir -p log; mkdir -p archive; mkdir -p tmp;"
cmd2="cd $workdir/$repos ; sudo bash dep.sh; sudo pip3 install -r requirements.txt"
cmd3=""
cmd4="cd $workdir/$repos ; sudo bash batch_kill.sh"
cmd5=""

if [ $1 == 'scp' ]; then
	eval $cmd1 
elif [ $1 == 'dep' ]; then
	eval $cmd2 
elif [ $1 == 'init' ]; then
    :
elif [ $1 == 'kill' ]; then
	eval $cmd4
elif [ $1 == 'sync_tmp' ]; then
    :
else
  :
fi


# sync to others
cmd1="mkdir -p $workdir; cd $workdir ; sudo rm -rf $repos; scp -r $USER@$c1:$workdir/$repos ."
cmd2="cd $workdir/$repos ; sudo bash dep.sh; sudo pip3 install -r requirements.txt"
cmd3="cd $workdir/$repos ; sudo bash init.sh "
cmd4="cd $workdir/$repos ; sudo bash batch_kill.sh"
cmd5="cd $workdir/$repos/tmp; scp -r $USER@$c1:$workdir/$repos/tmp/* ."
cmd6=""

for host in ${servers[@]}
do
  if [ $1 == 'scp' ]; then
    echo "scp to $host cmd: $cmd1"
    ssh $host "$cmd1" &
  elif [ $1 == 'dep' ]; then
    echo "scp to $host cmd: $cmd2"
    ssh $host "$cmd2" &
elif [ $1 == 'init' ]; then
    echo "init env $host cmd: $cmd3"
    ssh $host "$cmd3" &
elif [ $1 == 'kill' ]; then
    echo "kill depfast $host cmd: $cmd4"
    ssh $host "$cmd4"
elif [ $1 == 'sync_tmp' ]; then
    echo "sync tmp $host cmd: $cmd5"
    ssh $host "$cmd5" &
elif [ $1 == 'random' ]; then
    echo "random $host, cmd: $2"
    ssh $host "$2" 
    echo -e "\n"
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

