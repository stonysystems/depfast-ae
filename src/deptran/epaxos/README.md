# Epaxos C++ Implementation

This is an C++ implementation of optimized version of Epaxos replication protocol. 

* Each server is run as a seperate thread.

* The implementation assumes transitive dependency (a --> b, b --> c implies a --> c). The assumption is that all requests with the same dependency key are interfering with each other.

* The features like thrifty, batching, wide-area testing can be enabled if required.


# Running Wholistic Tests

The tests can run for any n-replica system where slow-path quorum and fast-path quorum are of different sizes (i.e replica systems with n>5). By default the N is set to 7 for tests. To change it, change `NSERVERS` in commo.h and use corresponding config file while running.

## To compile

```
python3 waf configure build --enable-epaxos-test
```

## To run

```
build/deptran_server -f config/epaxos_7r.yml > log.txt
```

# Running Performance Tests
The tests can run for any n-replica system. By default the N is set to 5. To change it, change `NSERVERS` in commo.h and use corresponding config file while running.

The performance tests run closed-loop tests, where a specified concurrency number (`<concurrency>`) of requests are processed by the system constantly till a specified total number of requests (`<total-requests>`) are processed. This is simulated as follows.
* Initially send the specified concurrency number of requests evenly to all replicas, then whenever a request is done processing (executed) then spawn a new request.

You can also adjust the percentage of conflicting requests by setting the `<conflict-percentage>`.

## To compile

### Default (no thrifty, no batching)
```
python3 waf configure build --enable-epaxos-perf-test
```

### Enable thrifty
When thrifty is enabled -
* the pre-accept and prepare requests will be send only to fast-path quorum of replicas
* the accept request will be send only to slow-path quorum of replicas

```
python3 waf configure build --enable-epaxos-perf-test --enable-thrifty
```

### Enable wide-area testing
Wide-area testing is simulated by sleeping for 40-50 ms before and after processing any RPC call (so total 80-100 ms delay).

```
python3 waf configure build --enable-epaxos-perf-test --enable-wide-area
```

## To run

```
build/deptran_server -f config/epaxos_5r.yml -n <concurrency> -T <total-requests> -o <conflict-percentage> > log.txt
```

For example,
```
build/deptran_server -f config/epaxos_5r.yml -n 10 -T 1000 -o 2 > log.txt
```

Here, concurrency refers to maximum how many requests will be concurrently processed by the system. So for example, if concurrency is 100 then at any point of time maximum 100 requests will be in-progress. New request will be started only when an in-progress request gets executed by the leader.

The total-requests refers to total number of requests that will be send in the test. 

The conflict-percentage denotes the percentage of interfering commands. Command interference is simulated by sending requests with same dependency key. For example, if conflict-percentage is 2, then 2%of the commands will target the same dependency key, while rest 98% of the commands will target unique keys. This is similar to how the Epaxos paper simulates command interference.

The generated logs will go to the log.txt file. By default, the log level is INFO. To change the log level to debug add -d option while compiling. But it is not preferred to enable debug level during performance tests as large number of file writes will reduce the throughput significantly.

## Output
The performance test output for each run will be stored in `root/plots/epaxos` folder as csv file with name format `latencies_<concurrency>_<total-requests>_<conflict-percentage>.csv`. The csv file can be imported to jupyter notebook for further analysis and graph generations. The csv file contains following data -
* Line 1: Comma seperated leader commit latencies of all commands
* Line 2: Comma seperated leader execution latencies of all commands
* Line 3: Average throughput 
* Line 4: Percentage of requests that took fast-path to commit
* Line 5: Total number of RPC calls

## Jupyter Notebook Code
The jupyter notebook code used for output analysis and graph creation is stored in `root/src/deptran/epaxos/jupyter-notebook` folder. You can import the file to your local jupyter-notebook and adjust the file path, total requests, concurrency and throughput values in the code as per your need.

