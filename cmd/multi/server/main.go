package main

import (
	"flag"
	"fmt"
	"ppfp/pkg/service"
	"ppfp/pkg/storage"
	"ppfp/pkg/txnexec"
)

//go:generate go run main.go -partitionNum 2 -partitionId 0 -port 30000 &
//go:generate go run main.go -partitionNum 2 -partitionId 1 -port 30001 &
func main() {

	partitionNum := flag.Uint64("partitionNum", 2, "Amount of partition number for this distributed system")
	partitionId := flag.Uint64("partitionId", 0, "Partition Id of this process")
	port := flag.String("port", "30000", "Service port number")
	flag.Parse()
	fmt.Println("PartitionNum: ", *partitionNum)
	fmt.Println("PartitionId: ", *partitionId)
	fmt.Println("Port: ", *port)

	te := txnexec.NewTxnExecutor(2048, 256, *partitionId, *partitionNum)
	s := storage.NewStorage(*partitionId, *partitionNum)

	server := service.NewTxnServer()
	go txnexec.VLL(te, te.TxnQueue, s, server.InputTxnCh)
	service.NewTxnService(*port, server)

}
