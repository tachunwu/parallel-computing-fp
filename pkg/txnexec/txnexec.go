package txnexec

import (
	"container/list"
	pb "ppfp/pkg/pb/txnpb"
	"ppfp/pkg/storage"
	"ppfp/pkg/throttle"
	"sync"

	"github.com/cespare/xxhash"
)

func (te *TxnExecutor) IsLocalKey(key []byte) bool {
	return xxhash.Sum64(key)%uint64(te.partitionNum) == te.partitionId
}

func VLL(te *TxnExecutor, tq *TxnQueue, s *storage.Storage, newTxnCh <-chan *pb.Txn) {

	for {
		if tq.queue.Len() != 0 && tq.queue.Front().Value.(pb.Txn).TxnType == pb.TxnType_BLOCKED {

			t := tq.queue.Front().Value.(*pb.Txn)
			if t.IsDistributed {
				t.TxnType = pb.TxnType_WAITING
			} else {
				t.TxnType = pb.TxnType_FREE
				te.throttle.Do()
				go s.Execute(t, te.throttle)
				te.FinishTxn(t)
			}
		} else if !tq.isFull {
			// Get new txn
			t := <-newTxnCh
			te.BeginTxn(t)
			if t.TxnType == pb.TxnType_FREE {
				te.throttle.Do()
				go s.Execute(t, te.throttle)
				te.FinishTxn(t)
			} else if t.TxnType == pb.TxnType_WAITING {
				tq.Enqueue(t)
			} else if t.TxnType == pb.TxnType_BLOCKED {
				tq.Enqueue(t)
			}
		}
	}
}

type TxnExecutor struct {
	lock         sync.Mutex
	throttle     *throttle.Throttle
	TxnQueue     *TxnQueue
	Cs           sync.Map
	Cx           sync.Map
	partitionId  uint64
	partitionNum uint64
}

func NewTxnExecutor(txnQueueMaxSize int, poolSize int, partitionId uint64, partitionNum uint64) *TxnExecutor {
	return &TxnExecutor{
		TxnQueue:     NewTxnQueue(txnQueueMaxSize),
		throttle:     throttle.NewThrottle(poolSize),
		partitionId:  partitionId,
		partitionNum: partitionNum,
	}
}

func (te *TxnExecutor) BeginTxn(txn *pb.Txn) {
	te.lock.Lock()
	txn.TxnType = pb.TxnType_FREE

	for _, key := range txn.ReadSet {
		if te.IsLocalKey(key) {
			cs, loaded := te.Cs.LoadOrStore(key, 1)
			if loaded {
				te.Cs.Store(key, cs.(int)+1)
			}
			cx, ok := te.Cx.Load(key)
			if ok && cx.(int) > 0 {
				txn.TxnType = pb.TxnType_BLOCKED
			}
		}
	}

	for _, key := range txn.WriteSet {
		if te.IsLocalKey(key) {
			cx, loaded := te.Cx.LoadOrStore(key, 1)
			if loaded {
				te.Cs.Store(key, cx.(int)+1)
			}

			cs, ok := te.Cs.Load(key)
			if ok && (cx.(int) > 1 || cs.(int) > 0) {
				txn.TxnType = pb.TxnType_BLOCKED
			}
		}
		if txn.IsDistributed && txn.TxnType == pb.TxnType_FREE {
			txn.TxnType = pb.TxnType_WAITING
		}
	}
	te.TxnQueue.Enqueue(txn)
	te.lock.Unlock()
}

func (te *TxnExecutor) FinishTxn(txn *pb.Txn) {
	te.lock.Lock()
	for _, key := range txn.ReadSet {
		if te.IsLocalKey(key) {
			cs, ok := te.Cs.Load(key)
			if ok {
				te.Cs.Store(key, cs.(int)-1)
			}
		}
	}

	for _, key := range txn.WriteSet {
		if te.IsLocalKey(key) {
			cx, ok := te.Cx.Load(key)
			if ok {
				te.Cs.Store(key, cx.(int)-1)
			}
		}
	}
	te.TxnQueue.Remove(txn)
	te.lock.Unlock()
}

type TxnQueue struct {
	isFull  bool
	maxSize int
	queue   *list.List
}

func NewTxnQueue(maxSize int) *TxnQueue {
	return &TxnQueue{
		maxSize: maxSize,
		isFull:  false,
		queue:   list.New(),
	}
}

func (tq *TxnQueue) Enqueue(txn *pb.Txn) {
	if tq.queue.Len() < tq.maxSize {
		tq.queue.PushBack(txn)
	} else {
		tq.isFull = true
	}
}

func (tq *TxnQueue) Remove(txn *pb.Txn) {
	if tq.queue.Len() > 0 {
		tq.queue.Remove(tq.queue.Front())
	}
}
