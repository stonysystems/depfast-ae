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

### Enable batching
When batching is enabled all requests (belonging to the same dependency key) received in 10 microseconds are batched together into a single request. This is different from the batching implemented by Epaxos paper. In the paper all requests (irrespective of dependency key) received in 5 milliseconds are batched together into a single request.

```
python3 waf configure build --enable-epaxos-perf-test --enable-batching
```

## To run

```
build/deptran_server -f config/epaxos_5r.yml -n <concurrency> -T <total-requests> -o <conflict-percentage> > log.txt
```

For example,
```
build/deptran_server -f config/epaxos_5r.yml -n 10 -T 1000 -o 2 > log.txt
```

# Weak Consistency
Weak consistency was simulated by executing a request as soon as it is committed without checking the dependency graphs. 

NOTE: This is not an ideal implementation but rather a simulation.

## To compile

```
python3 waf configure build --enable-epaxos-perf-test --enable-epaxos-eventual-test
```

## To run

```
build/deptran_server -f config/epaxos_5r.yml -n <concurrency> -T <total-requests> -o <conflict-percentage> > log.txt
```
