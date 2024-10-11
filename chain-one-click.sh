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
    for host in ${servers3[@]}
    do
        ssh $host "pkill deptran_server"
    done
}

experimenta() {
  conc=( 1 2 3 4 5 6 7 8 9 10 )
  conc=( 10 )

  filename=experimenta
  mkdir -p $filename
  rm -rf $filename

  # python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=1 --enable-single-path=1
  # 92-95%
  # 10: 26406.7 (sometimes: 23977.1 or 24707.7)

  # chainRPC
  # timeout: 5ms
  # 10: 25612.9 (M 200*10000)
  # 10: 16071.2 (M 2000)
  # 10: 16263 (M 10000)

  # timeout: 1ms
  # 10: 18444.9 (M 10000)

  # timeout: 100ms
  # 10: 16922.5 (M 10000)

  # timeout: 10000000ms
  # 10: 16684.9 (M 10000)

  # timeout: 10000000ms
  # 10: 19081.1 (M 10*10000)
  # 85-90%
  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=1 --enable-single-path=0
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_3_$i
      mv *.log ./$filename/results_3_$i
    done

  # baseline Raft
  # 10: 33733.4
  # 99%, 30%, 75% --> ok
  python3 waf configure -J build --enable-chainrpc=0 --in-order-enforce=0 --enable-single-path=0
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
  conc=( 1 2 3 4 5 6 7 8 9 10 )
  conc=( 10 )

  filename=experimentb
  mkdir -p $filename
  rm -rf $filename

  # chainRPC (without in-order enforcement, we retransmit rejected requests)
  # 10: 12795.5
  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=0 --enable-single-path=0
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
  echo "the same as experimenta with latency monitor."
  # Do nothing here!
}

experimentd() {
  echo "the same as experimenta, but with slowness. Put a slowness on one node, and then monitor performance changes."

  conc=( 1 2 3 4 5 6 7 8 9 10 )
  conc=( 10 )

  filename=experimentd
  mkdir -p $filename
  rm -rf $filename

  # chainRPC
  # 10: 3541.35
  python3 waf configure -J build --enable-chainrpc=1 --in-order-enforce=1 --enable-single-path=0
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i slow"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_3_$i
      mv *.log ./$filename/results_3_$i
    done

  # baseline Raft - with slowness
  # 10: 33500.2
  python3 waf configure -J build --enable-chainrpc=0 --in-order-enforce=0 --enable-single-path=0
  for i in "${conc[@]}"
    do
      cmd="./chain_run3.sh $i slow"
	    Kill
      eval $cmd
      mkdir -p ./$filename/results_3_slowness_$i
      mv *.log ./$filename/results_3_slowness_$i
    done
}

experimente() {
  echo "Port to Rolis."
}

experimenta
experimentb
experimentc
experimentd
experimente
