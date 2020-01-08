Mongodb Benchmarking for Replicaset
===================================

This script is used to test the performance of the replicaset clusters and focused only on write queries.

Requirements
------------
- Python3
- pymongo

Details
-------

1. Test the mongodb connection.
```
    python3 mongodb_benchmark.py -t
```

2. Run the queries in serial(one by one), it helps to find out the execution time for the query on the environment.
```
    python3 mongodb_benchmark.py -m serial -n 300
```

3. Run the queries in parallel(multiple threads based on cpu count).
```
    python3 mongodb_benchmark.py -m parallel -n 300
```
