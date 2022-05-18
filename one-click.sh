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


ONLY_CMD=1
SLOWDOWN_DUR=180
SLOWDOWN_DUR_EXP=205
TUPT_DUR=60
TUPT_DUR_EXP=75

ulimit -n 10000

setup () {
    if [ $ONLY_CMD -eq 0 ]
    then
      bash ./batch_op.sh kill
    fi
}

build_scp() {
  python3 waf configure build
  bash ./batch_op.sh init
  bash ./batch_op.sh scp
}

timeout_process() {
  cmd=$1
  waitTime=$2
  rerun=$3
  eval $cmd &
  myPid=$!

  sleep $waitTime
  if kill -0 "$myPid"; then
    # still alive, kill it then re-run it
    kill -9 "$myPid"
    if [ $rerun -eq 1 ]
    then
       timeout_process "$cmd" $waitTime 0
    fi
  else
    echo ""
  fi
}

# figure5a:
#  1. fail-slow on followers with fpga_raft
#  2. no slowdown
#  3. replicas: 3, 5
#  4. fix # of client and then vary # of concurrent
experiment1() {
    if [ $ONLY_CMD -eq 0 ]
    then
      build_scp
    fi
    mkdir -p ./figure5a
    rm -rf ./figure5a/*

    rm -rf ./results
    # 3 replicas
    conc=( 20 40 60 80 100 130 160 190 220 260 300 340 380 420 )
    conc=( 20 40 ) # testing
    for i in "${conc[@]}"
    do
      mkdir results
      cmd="./start-exp.sh testname $TUPT_DUR 0 3 follower 1 $i fpga_raft nonlocal"
      if [ $ONLY_CMD -eq 1 ]
      then
        echo $cmd
      else
        eval $cmd &
        timeout_process "$cmd" $TUPT_DUR_EXP 1
      fi
      mv results ./figure5a/results_3_$i
    done

    # 5 replicas
    for i in "${conc[@]}"
    do
      mkdir results
      cmd="./start-exp.sh testname $TUPT_DUR 0 5 follower 1 $i fpga_raft nonlocal"
      if [ $ONLY_CMD -eq 1 ]
      then
        echo $cmd
      else
        timeout_process "$cmd" $TUPT_DUR_EXP 1
      fi
      mv results ./figure5a/results_5_$i
    done
}

# figure5b:
#  1. fail-slow on followers with fpga_raft
#  2. with 6 slowdown types
#  3. replicas: 3, 5
experiment2() {
    if [ $ONLY_CMD -eq 0 ]
    then
      build_scp
    fi
    mkdir -p ./figure5b
    rm -rf ./figure5b/*

    rm -rf ./results
    # 3 replicas
    exp=( 1 2 3 4 5 6 )
    for i in "${exp[@]}"
    do
      mkdir results
      cmd="./start-exp.sh testname $SLOWDOWN_DUR $i 3 follower 1 200 fpga_raft nonlocal"
      if [ $ONLY_CMD -eq 1 ]
      then
        echo $cmd
      else
        timeout_process "$cmd" $SLOWDOWN_DUR_EXP 1
      fi
      mv results ./figure5b/results_3_$i
    done

    # 5 replicas
    for i in "${exp[@]}"
    do
      mkdir results
      cmd="./start-exp.sh testname $SLOWDOWN_DUR $i 5 follower 1 200 fpga_raft nonlocal"
      if [ $ONLY_CMD -eq 1 ]
      then
        echo $cmd
      else
        timeout_process "$cmd" $SLOWDOWN_DUR_EXP 1
      fi
      mv results ./figure5b/results_5_$i
    done
}

# figure5c: based on figure5b
experiment3() {
  echo ""
}

# figure5d: based on figure5b
experiment4() {
  echo ""
}

# figure6a:
#  1. 3 replica setting on copilot
#  2. no slowdown
#  3. fix # of client and then vary # of concurrent
experiment5() {
  if [ $ONLY_CMD -eq 0 ]
  then
    build_scp
  fi
  mkdir -p ./figure6a
  rm -rf ./figure6a/*

  rm -rf ./results
  conc=( 1 2 4 6 8 10 12 14 16 18 20 )
  conc=( 2 4 ) # testing
  for i in "${conc[@]}"
  do
    mkdir results
    cmd="./start-exp.sh testname $TUPT_DUR 0 3 follower 1 $i copilot nonlocal"
    if [ $ONLY_CMD -eq 1 ]
    then
      echo $cmd
    else
      timeout_process "$cmd" $TUPT_DUR_EXP 1
    fi
    mv results ./figure6a/results_$i
  done
}

# figure6b:
#  1. 3-replica setting on copilot
#  2. slowdown type: 1, 2, 5, 6
#  3. on follower and leader
experiment6() {
  if [ $ONLY_CMD -eq 0 ]
  then
    build_scp
  fi
  mkdir -p ./figure6b
  rm -rf ./figure6b/*

  rm -rf ./results
  exp=( 1 2 5 6 )
  # on the leader
  for i in "${exp[@]}"
  do
    mkdir results
    cmd="./start-exp.sh testname $SLOWDOWN_DUR $i 3 leader 1 12 copilot nonlocal"
    if [ $ONLY_CMD -eq 1 ]
    then
      echo $cmd
    else
      timeout_process "$cmd" $SLOWDOWN_DUR_EXP 1
    fi
    mv results ./figure6b/results_leader_$i
  done

  # on the follower
  for i in "${exp[@]}"
  do
    mkdir results
    cmd="./start-exp.sh testname $SLOWDOWN_DUR $i 3 follower 1 12 copilot nonlocal"
    if [ $ONLY_CMD -eq 1 ]
    then
      echo $cmd
    else
      timeout_process "$cmd" $SLOWDOWN_DUR_EXP 1
    fi
    mv results ./figure6b/results_follower_$i
  done
}

# figure6c: based on figure6b
experiment7() {
  echo ""
}

# figure6d: based on figure6b
experiment8() {
  echo ""
}

setup

experiment1
echo -e "experiment-1\n"

experiment2
echo -e "experiment-2\n"

experiment3
echo -e "experiment-3\n"

experiment4
echo -e "experiment-4\n"

experiment5
echo -e "experiment-5\n"

experiment6
echo -e "experiment-6\n"

experiment7
echo -e "experiment-7\n"

experiment8
echo -e "experiment-8\n"  