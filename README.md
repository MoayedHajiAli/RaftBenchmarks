# Raft Key-Value benchmarks 
This benchmark is based on a raft library implemented in python called [PySyncObj](https://github.com/bakwc/PySyncObj)

 > PySyncObj is a python library for building fault-tolerant distributed systems. It provides the ability to replicate your application data between multiple servers.
 
# install
PySyncObj
 
```
pip3 install pysyncobj
```

# usage on local testing
### Latency test on local machine:
Run Delay_becnhmark.py and specify the number of servers on the cluster, the number of requests per seconds, the starting packet size, the ending packet size, and the number of tests.

The program will result in a graph (packet size - latency) and two log files (server-side log and client-side log)

Example:
```
python3 Delay_benchmark.py  3 500 10 100 3
```
Result:
![alt text](https://i.ibb.co/q5Qgb35/Screenshot-2019-09-22-at-21-55-22.png)



### For Running a separated local test:
- Run StartLocalServers.py and specify the number of servers in the cluster
- Latency: run Single_delay_benchmark.py and specify RPS, key packet size, value packet size

- Throughput: run Single_Throughput_benchmark.py and specify the number of iterations to be tested on, RPS, key packet size, value packet size

- RPS: run Single_RPS_benchmark.py and specify RPS, key packet size, value packet size 

###For Running the benchmarks on different machines:
- Run Kvtestobj.py to launch a new server and pass the client port, the address of the current server, and the addresses of the other servers in the cluster in an array format
- Please specify the client port to be the port used to connect other servers in the cluster plus one
- Edit the SERVERS_IP array in the desired benchmark to the new servers IPs (that were open for clients) and run the desired benchmark on the client machine

Example:
```
Server1: python3 Kvtestobj.py 4322 localhost:4321 localhost:4323 localhost:4325
Server2: python3 Kvtestobj.py 4324 localhost:4323 localhost:4321 localhost:4325
Server3: python3 Kvtestobj.py 4326 localhost:4325 localhost:4321 localhost:4323
Change the SERVERS_IP to SERVERS_IP = ['localhost:4322', 'localhost:4324', 'localhost:4326']
Client: python3 Single_delay_benchmark.py 500 100 100
```
