# Parallel Programming Final Project
## Objective
In this final project, I tried to implement a distributed ACID key-value database. To achieve these objectives I reference two important papers <**Calvin: Fast Distributed Transactions for Partitioned Database Systems**> and <**VLL: A Lock Manager Redesign for Main Memory Database Systems**>.
These papers' concept can build a distributed system with high throughput, ACID transaction. If you want to know about local concurrency management check the first paper. On the other hand, if you want to know more about deterministic distributed transactions check the second paper.

## Run
### Pre-require
Make sure you have **Go**, **Protocol Buffer Compiler** in your local environment.
### Build Protocol Buffer
```
cd proto
protoc --go_out=../pkg --go-grpc_out=../pkg txn.proto
```
### Single Process
```
cd cmd/single
go run main.go
```
### Multi Process
#### Server
```
cd cmd/multi/server
go run main.go -partitionNum 2 -partitionId 0 -port 30000
go run main.go -partitionNum 2 -partitionId 1 -port 30001
```
#### Client
```
cd cmd/multi/client go run main.go
```
## Benchmark
### Single Process
```
wudajunde-MBP:single wudajun$ go run main.go
==============================
Benchmark: Write 0% Read 100%
Total time:  4.355001995s
TPS:  229621.01995546845
==============================
Benchmark: Write 100% Read 0%
Total time:  33.945988564s
TPS:  29458.561741828555
==============================
Benchmark: Write 75% Read 25%
Total time:  26.94618302s
TPS:  37111.007494374244
==============================
Benchmark: Write 50% Read 50%
Total time:  19.599092084s
TPS:  51022.771652589174
==============================
Benchmark: Write 25% Read 75%
Total time:  12.516809665s
TPS:  79892.56262290539
```
### Multi Process
```
wudajunde-MBP:client wudajun$ go run main.go
==============================
Benchmark: Write 100% Read 0%
Total time:  2.862610677s
TPS:  34933.147145527815
==============================
Benchmark: Write 75% Read 25%
Total time:  3.073878403s
TPS:  32532.191222139245
==============================
Benchmark: Write 50% Read 50%
Total time:  2.711997726s
TPS:  36873.18726018725
==============================
Benchmark: Write 25% Read 75%
Total time:  2.900772638s
TPS:  34473.57393337354
==============================
Benchmark: Write 0% Read 100%
Total time:  2.932249736s
TPS:  34103.507205499496
```
## Author
Tachun Wu with LSD
