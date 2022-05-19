repos="depfast"  # repos name, default
workdir="~/code"  # we default put our repos under the root

eval cd $workdir/$repos/data_processing
python3 plot_raft.py
python3 plot_copilot.py
