perf script > out.perf

# git clone github.com/brendangregg/FlameGraph.git
cd FlameGraph
./stackcollapse-perf.pl ../out.perf > out.folded
./flamegraph.pl out.folded > flamegraph.svg

