package storage

import (
	"log"
	pb "ppfp/pkg/pb/txnpb"
	"ppfp/pkg/throttle"
	"strconv"

	"github.com/cespare/xxhash"
	"github.com/cockroachdb/pebble"
)

type Storage struct {
	kv           *pebble.DB
	partitionId  uint64
	partitionNum uint64
}

func (s *Storage) IsLocalKey(key []byte) bool {
	return xxhash.Sum64(key)%uint64(s.partitionNum) == s.partitionId
}

func NewStorage(partitionId uint64, partitionNum uint64) *Storage {
	db, err := pebble.Open("./data"+strconv.Itoa(int(partitionId)), &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	return &Storage{
		kv:           db,
		partitionId:  partitionId,
		partitionNum: partitionNum,
	}
}

func (s *Storage) Execute(t *pb.Txn, th *throttle.Throttle) {
	defer th.Done(nil)
	txnRes := pb.TxnResponse{
		Id: t.Id,
	}
	batch := s.kv.NewIndexedBatch()
	for _, op := range t.Operations {
		if s.IsLocalKey(op.Key) {
			switch op.OperationType {
			case pb.OperationType_GET:

				res, closer, err := batch.Get(op.Key)

				if err != nil {
					// Response
					txnRes.Kvs = append(
						txnRes.Kvs,
						&pb.KeyValue{
							Key:   op.Key,
							Value: nil,
						},
					)
				} else {
					// Response
					txnRes.Kvs = append(
						txnRes.Kvs,
						&pb.KeyValue{
							Key:   op.Key,
							Value: res,
						},
					)

					if err := closer.Close(); err != nil {
						log.Println(err)
					}
				}

			case pb.OperationType_SET:
				if err := batch.Set(
					op.Key,
					op.Value,
					pebble.Sync,
				); err != nil {
					log.Println(err)
				}
				// Response
				txnRes.Kvs = append(
					txnRes.Kvs,
					&pb.KeyValue{
						Key:   op.Key,
						Value: op.Value,
					},
				)
			case pb.OperationType_DELETE:
				err := batch.Delete(op.Key, pebble.Sync)
				if err != nil {
					log.Println(err)
				}
				// Response
				txnRes.Kvs = append(
					txnRes.Kvs,
					&pb.KeyValue{
						Key:   op.Key,
						Value: nil,
					},
				)
			}
		}

	}
	s.kv.Apply(batch, pebble.Sync)
	// log.Println(txnRes)
}
