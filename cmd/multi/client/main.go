package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	pb "ppfp/pkg/pb/txnpb"
	"ppfp/pkg/throttle"

	"google.golang.org/grpc"
)

func main() {
	th := throttle.NewThrottle(256)

	connP0, err := grpc.Dial("localhost:30000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer connP0.Close()
	connP1, err := grpc.Dial("localhost:30001", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer connP1.Close()
	clientP0 := pb.NewTxnServiceClient(connP0)
	clientP1 := pb.NewTxnServiceClient(connP1)
	BenchmarkW100R0(clientP0, clientP1, *th)
	BenchmarkW75R25(clientP0, clientP1, *th)
	BenchmarkW50R50(clientP0, clientP1, *th)
	BenchmarkW25R75(clientP0, clientP1, *th)
	BenchmarkW0R100(clientP0, clientP1, *th)
}

var txnNum int = 100000

func BenchmarkW100R0(c0 pb.TxnServiceClient, c1 pb.TxnServiceClient, th throttle.Throttle) {
	start := time.Now()
	for i := 0; i <= txnNum; i++ {
		th.Do()
		go func() {
			defer th.Done(nil)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

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
			if i%2 == 0 {
				_, err := c0.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			} else {
				_, err := c1.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			}
		}()

	}
	end := time.Now()
	fmt.Println("==============================")
	fmt.Println("Benchmark: Write 100% Read 0%")
	fmt.Println("Total time: ", end.Sub(start))
	fmt.Println("TPS: ", float64(txnNum)/end.Sub(start).Seconds())
}

func BenchmarkW0R100(c0 pb.TxnServiceClient, c1 pb.TxnServiceClient, th throttle.Throttle) {
	start := time.Now()
	for i := 0; i <= txnNum; i++ {
		th.Do()
		go func(i int) {
			defer th.Done(nil)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
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
			if i%2 == 0 {
				_, err := c0.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			} else {
				_, err := c1.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			}
		}(i)

	}
	end := time.Now()
	fmt.Println("==============================")
	fmt.Println("Benchmark: Write 0% Read 100%")
	fmt.Println("Total time: ", end.Sub(start))
	fmt.Println("TPS: ", float64(txnNum)/end.Sub(start).Seconds())
}

func BenchmarkW50R50(c0 pb.TxnServiceClient, c1 pb.TxnServiceClient, th throttle.Throttle) {
	start := time.Now()
	for i := 0; i <= txnNum/2; i++ {

		th.Do()
		go func() {
			defer th.Done(nil)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
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
			if i%2 == 0 {
				_, err := c0.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			} else {
				_, err := c1.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			}
		}()

	}
	for i := 0; i <= txnNum/2; i++ {

		th.Do()
		go func() {
			defer th.Done(nil)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
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
			if i%2 == 0 {
				_, err := c0.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			} else {
				_, err := c1.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			}
		}()
	}
	end := time.Now()
	fmt.Println("==============================")
	fmt.Println("Benchmark: Write 50% Read 50%")
	fmt.Println("Total time: ", end.Sub(start))
	fmt.Println("TPS: ", float64(txnNum)/end.Sub(start).Seconds())
}

func BenchmarkW75R25(c0 pb.TxnServiceClient, c1 pb.TxnServiceClient, th throttle.Throttle) {
	start := time.Now()
	for i := 0; i <= txnNum*3/4; i++ {

		th.Do()
		go func() {
			defer th.Done(nil)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
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
			if i%2 == 0 {
				_, err := c0.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			} else {
				_, err := c1.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			}
		}()

	}
	for i := 0; i <= txnNum/4; i++ {

		th.Do()
		go func() {
			defer th.Done(nil)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
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
			if i%2 == 0 {
				_, err := c0.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			} else {
				_, err := c1.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			}
		}()
	}
	end := time.Now()
	fmt.Println("==============================")
	fmt.Println("Benchmark: Write 75% Read 25%")
	fmt.Println("Total time: ", end.Sub(start))
	fmt.Println("TPS: ", float64(txnNum)/end.Sub(start).Seconds())
}

func BenchmarkW25R75(c0 pb.TxnServiceClient, c1 pb.TxnServiceClient, th throttle.Throttle) {
	start := time.Now()
	for i := 0; i <= txnNum/4; i++ {

		th.Do()
		go func() {
			defer th.Done(nil)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
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
			if i%2 == 0 {
				_, err := c0.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			} else {
				_, err := c1.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			}
		}()

	}
	for i := 0; i <= txnNum*3/4; i++ {

		th.Do()
		go func() {
			defer th.Done(nil)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
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
			if i%2 == 0 {
				_, err := c0.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			} else {
				_, err := c1.CreateTxn(ctx,
					&pb.TxnRequest{
						Txn: newTxn,
					},
				)
				if err != nil {
					log.Println(err)
				}
			}
		}()
	}
	end := time.Now()
	fmt.Println("==============================")
	fmt.Println("Benchmark: Write 25% Read 75%")
	fmt.Println("Total time: ", end.Sub(start))
	fmt.Println("TPS: ", float64(txnNum)/end.Sub(start).Seconds())
}
