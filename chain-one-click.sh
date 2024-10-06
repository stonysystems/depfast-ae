# nload enp129s0f0

servers3=(
  "192.168.1.102" # leader
  "192.168.1.103" # p1
  "192.168.1.104" # p2
  "192.168.1.102" # client
)

Kill() {
    for host in ${servers3[@]}
    do
        ssh $host "pkill deptran_server"
    done
}


experimenta() {
  conc=( 20 40 60 80 100 130 160 190 200 220 260 300 340 380 420 )
  conc=( 20 )

  filename=experimenta
  mkdir -p $filename
  rm -rf $filename

  # chainRPC
  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=1
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_3_$i
      mv *.log ./$filename/results_3_$i
    done

  # normal Raft
  python3 waf configure -J build --enable-chainrpc=0 --in-order-enforce=0
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_3_disablechain_$i
      mv *.log ./$filename/results_3_disablechain_$i
    done
}

experimentb() {
  conc=( 20 40 60 80 100 130 160 190 200 220 260 300 340 380 420 )
  conc=( 20 )

  filename=experimentb
  mkdir -p $filename
  rm -rf $filename

  # chainRPC (in-order enforcement)
  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=1
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_3_$i
      mv *.log ./$filename/results_3_$i
    done

  # chainRPC (without in-order enforcement, we retransmit rejected requests)
  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=0
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_3_disableinorder_$i
      mv *.log ./$filename/results_3_disableinorder_$i
    done
}

experimentc() {
  echo ""
}

experimentd() {
  echo ""
}

experimente() {
  echo ""
}

#experimenta
experimentb
experimentc
experimentd
experimente
