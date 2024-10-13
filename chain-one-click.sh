# nload enp129s0f0
# awk '/Throughput:/ {match($0, /Throughput: ([0-9.]+)/, a); sum += a[1]} END {print sum}' *.log
# Log_track --> Log_info

servers3=(
  "192.168.1.102" # leader
  "192.168.1.103" # p1
  "192.168.1.104" # p2
  "192.168.1.102" # client
)

Kill() {
    ./kill.sh
    rm -rf *.log
}

experiment_raft_3() {
  suffix=$1
  conc=( 1 2 3 4 5 6 7 8 9 10 )
  
  filename=experiment_raft_3
  mkdir -p $filename

  python3 waf configure -J build --enable-chainrpc=0 --in-order-enforce=0 --enable-single-path=0
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i"
	    echo "Run $cmd"
      Kill
      eval $cmd
      mkdir -p ./$filename/results_3_$i-$suffix
      rm -rf ./$filename/results_3_$i-$suffix/*
      mv *.log ./$filename/results_3_$i-$suffix
    done
}

experiment_raft_5() {
  suffix=$1
  conc=( 1 2 3 4 5 6 7 8 9 10 )

  filename=experiment_raft_5
  mkdir -p $filename

  python3 waf configure -J build --enable-chainrpc=0 --in-order-enforce=0 --enable-single-path=0
  for i in "${conc[@]}"
    do
      cmd="./chain_run5.sh $i"
      echo "Run $cmd"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_5_$i-$suffix
      rm -rf ./$filename/results_5_$i-$suffix/*
      mv *.log ./$filename/results_5_$i-$suffix
    done
}

experiment_single_path_3() {
  suffix=$1
  conc=( 1 2 3 4 5 6 7 8 9 10 )

  filename=experiment_single_path_3
  mkdir -p $filename

  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=1 --enable-single-path=1
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i"
      echo "Run $cmd"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_3_$i-$suffix
      rm -rf ./$filename/results_3_$i-$suffix/*
      mv *.log ./$filename/results_3_$i-$suffix
    done
}


experiment_single_path_5() {
  suffix=$1
  conc=( 1 2 3 4 5 6 7 8 9 10 )

  filename=experiment_single_path_5
  mkdir -p $filename

  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=1 --enable-single-path=1
  for i in "${conc[@]}"
    do
      cmd="./chain_run5.sh $i"
      echo "Run $cmd"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_5_$i-$suffix
      rm -rf ./$filename/results_5_$i-$suffix/*
      mv *.log ./$filename/results_5_$i-$suffix
    done
}

experiment_chainrpc_3() {
  suffix=$1
  conc=( 1 2 3 4 5 6 7 8 9 10 )

  filename=experiment_chainrpc_3
  mkdir -p $filename

  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=1 --enable-single-path=0
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i"
      echo "Run $cmd"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_3_$i-$suffix
      rm -rf ./$filename/results_3_$i-$suffix/*
      mv *.log ./$filename/results_3_$i-$suffix
    done
}


experiment_chainrpc_5() {
  suffix=$1
  conc=( 1 2 3 4 5 6 7 8 9 10 )

  filename=experiment_chainrpc_5
  mkdir -p $filename

  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=1 --enable-single-path=0
  for i in "${conc[@]}"
    do
      cmd="./chain_run5.sh $i"
      echo "Run $cmd"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_5_$i-$suffix
      rm -rf ./$filename/results_5_$i-$suffix/*
      mv *.log ./$filename/results_5_$i-$suffix
    done
}


experiment_chainrpc_noorder_3() {
  suffix=$1
  conc=( 1 2 3 4 5 6 7 8 9 10 )

  filename=experiment_chainrpc_noorder_3
  mkdir -p $filename

  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=0 --enable-single-path=0
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i"
      echo "Run $cmd"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_3_$i-$suffix
      rm -rf ./$filename/results_3_$i-$suffix/*
      mv *.log ./$filename/results_3_$i-$suffix
    done
}


experiment_chainrpc_noorder_5() {
  suffix=$1
  conc=( 1 2 3 4 5 6 7 8 9 10 )

  filename=experiment_chainrpc_noorder_5
  mkdir -p $filename

  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=0 --enable-single-path=0
  for i in "${conc[@]}"
    do
      cmd="./chain_run5.sh $i"
      echo "Run $cmd"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_5_$i-$suffix
      rm -rf ./$filename/results_5_$i-$suffix/*
      mv *.log ./$filename/results_5_$i-$suffix
    done
}

experiment_chainrpc_slow_3() {
  suffix=$1
  conc=( 1 2 3 4 5 6 7 8 9 10 )

  filename=experiment_chainrpc_slow_3
  mkdir -p $filename

   python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=1 --enable-single-path=0 --enable-slowness=1
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i"
      echo "Run $cmd"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_3_$i-$suffix
      rm -rf ./$filename/results_3_$i-$suffix/*
      mv *.log ./$filename/results_3_$i-$suffix
    done
}

experiment_chainrpc_slow_5() {
  suffix=$1
  conc=( 1 2 3 4 5 6 7 8 9 10 )

  filename=experiment_chainrpc_slow_5
  mkdir -p $filename

  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=1 --enable-single-path=0 --enable-slowness=1
  for i in "${conc[@]}"
    do
      cmd="./chain_run5.sh $i slow"
      echo "Run $cmd"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_5_$i-$suffix
      rm -rf ./$filename/results_5_$i-$suffix/*
      mv *.log ./$filename/results_5_$i-$suffix
    done
}

# -----------------------------------------------------------------------------------------------

#experiment_raft_3 1
#experiment_raft_5 1

#experiment_single_path_3 1
#experiment_single_path_5 1

#experiment_chainrpc_3 1
#experiment_chainrpc_5 1

# experiment_chainrpc_noorder_3 1
# experiment_chainrpc_noorder_5 1

experiment_chainrpc_slow_3 1
experiment_chainrpc_slow_5 1