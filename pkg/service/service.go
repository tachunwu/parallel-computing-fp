package service

import (
	"context"
	"log"
	"net"
	pb "ppfp/pkg/pb/txnpb"

	"google.golang.org/grpc"
)

type TxnServer struct {
	InputTxnCh chan *pb.Txn
	pb.UnimplementedTxnServiceServer
}

func NewTxnServer() *TxnServer {
	return &TxnServer{
		InputTxnCh: make(chan *pb.Txn),
	}
}

func NewTxnService(port string, server *TxnServer) *TxnServer {
	lis, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	pb.RegisterTxnServiceServer(grpcServer, server)
	grpcServer.Serve(lis)
	return server
}

func (s *TxnServer) CreateTxn(ctx context.Context, req *pb.TxnRequest) (*pb.TxnResponse, error) {
	s.InputTxnCh <- req.GetTxn()
	return &pb.TxnResponse{}, nil
}
