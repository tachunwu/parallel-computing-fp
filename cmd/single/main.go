package main

import (
	"fmt"
	pb "ppfp/pkg/pb/txnpb"
	"ppfp/pkg/storage"
	"ppfp/pkg/txnexec"
	"strconv"
	"time"
)

func main() {
	//
	txnCh := make(chan *pb.Txn)
	te := txnexec.NewTxnExecutor(2048, 256, 0, 1)
	s := storage.NewStorage(0, 1)

	go txnexec.VLL(te, te.TxnQueue, s, txnCh)

	BenchmarkW0R100(txnCh)
	BenchmarkW100R0(txnCh)
	BenchmarkW75R25(txnCh)
	BenchmarkW50R50(txnCh)
	BenchmarkW25R75(txnCh)

}

func BenchmarkW100R0(inputCh chan<- *pb.Txn) {
	start := time.Now()
	for i := 0; i <= 1000000; i++ {
		newTxn := &pb.Txn{
			Id: int32(i),
			Operations: []*pb.Operation{
				{
					OperationType: pb.OperationType_SET,
					Key:           []byte(strconv.Itoa(i)),
					Value:         []byte(strconv.Itoa(i)),
				},
			},
		}
		inputCh <- newTxn
	}
	end := time.Now()
	fmt.Println("==============================")
	fmt.Println("Benchmark: Write 100% Read 0%")
	fmt.Println("Total time: ", end.Sub(start))
	fmt.Println("TPS: ", 1000000.0/end.Sub(start).Seconds())
}

func BenchmarkW0R100(inputCh chan<- *pb.Txn) {
	start := time.Now()
	for i := 0; i <= 1000000; i++ {
		newTxn := &pb.Txn{
			Id: int32(i),
			Operations: []*pb.Operation{
				{
					OperationType: pb.OperationType_GET,
					Key:           []byte(strconv.Itoa(i)),
					Value:         nil,
				},
			},
		}
		inputCh <- newTxn
	}
	end := time.Now()
	fmt.Println("==============================")
	fmt.Println("Benchmark: Write 0% Read 100%")
	fmt.Println("Total time: ", end.Sub(start))
	fmt.Println("TPS: ", 1000000.0/end.Sub(start).Seconds())
}

func BenchmarkW50R50(inputCh chan<- *pb.Txn) {
	start := time.Now()
	for i := 0; i <= 500000; i++ {
		newTxn := &pb.Txn{
			Id: int32(i),
			Operations: []*pb.Operation{
				{
					OperationType: pb.OperationType_SET,
					Key:           []byte(strconv.Itoa(i)),
					Value:         nil,
				},
			},
		}
		inputCh <- newTxn
	}
	for i := 0; i <= 500000; i++ {
		newTxn := &pb.Txn{
			Id: int32(i),
			Operations: []*pb.Operation{
				{
					OperationType: pb.OperationType_GET,
					Key:           []byte(strconv.Itoa(i)),
					Value:         nil,
				},
			},
		}
		inputCh <- newTxn
	}
	end := time.Now()
	fmt.Println("==============================")
	fmt.Println("Benchmark: Write 50% Read 50%")
	fmt.Println("Total time: ", end.Sub(start))
	fmt.Println("TPS: ", 1000000.0/end.Sub(start).Seconds())
}

func BenchmarkW75R25(inputCh chan<- *pb.Txn) {
	start := time.Now()
	for i := 0; i <= 750000; i++ {
		newTxn := &pb.Txn{
			Id: int32(i),
			Operations: []*pb.Operation{
				{
					OperationType: pb.OperationType_SET,
					Key:           []byte(strconv.Itoa(i)),
					Value:         nil,
				},
			},
		}
		inputCh <- newTxn
	}
	for i := 0; i <= 250000; i++ {
		newTxn := &pb.Txn{
			Id: int32(i),
			Operations: []*pb.Operation{
				{
					OperationType: pb.OperationType_GET,
					Key:           []byte(strconv.Itoa(i)),
					Value:         nil,
				},
			},
		}
		inputCh <- newTxn
	}
	end := time.Now()
	fmt.Println("==============================")
	fmt.Println("Benchmark: Write 75% Read 25%")
	fmt.Println("Total time: ", end.Sub(start))
	fmt.Println("TPS: ", 1000000.0/end.Sub(start).Seconds())
}

func BenchmarkW25R75(inputCh chan<- *pb.Txn) {
	start := time.Now()
	for i := 0; i <= 250000; i++ {
		newTxn := &pb.Txn{
			Id: int32(i),
			Operations: []*pb.Operation{
				{
					OperationType: pb.OperationType_SET,
					Key:           []byte(strconv.Itoa(i)),
					Value:         nil,
				},
			},
		}
		inputCh <- newTxn
	}
	for i := 0; i <= 750000; i++ {
		newTxn := &pb.Txn{
			Id: int32(i),
			Operations: []*pb.Operation{
				{
					OperationType: pb.OperationType_GET,
					Key:           []byte(strconv.Itoa(i)),
					Value:         nil,
				},
			},
		}
		inputCh <- newTxn
	}
	end := time.Now()
	fmt.Println("==============================")
	fmt.Println("Benchmark: Write 25% Read 75%")
	fmt.Println("Total time: ", end.Sub(start))
	fmt.Println("TPS: ", 1000000.0/end.Sub(start).Seconds())
}
