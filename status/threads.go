package status

import "sync/atomic"

type ThreadStatus uint32

func (ts ThreadStatus) IsWorking() bool {
	return ts == 1
}

type ThreadsDetail struct {
	status  []uint32
	counter []uint64
}

func (td *ThreadsDetail) Working(tid uint64) {
	atomic.StoreUint32(&td.status[tid], 1)
}

func (td *ThreadsDetail) Idle(tid uint64) {
	atomic.StoreUint32(&td.status[tid], 0)
}

func (td *ThreadsDetail) Status(tid uint) ThreadStatus {
	return ThreadStatus(atomic.LoadUint32(&td.status[tid]))
}

func (td *ThreadsDetail) Add(tid uint64, n uint64) {
	atomic.AddUint64(&td.counter[tid], n)
}

func (td *ThreadsDetail) Count(tid uint64) uint64 {
	return atomic.LoadUint64(&td.counter[tid])
}

func (td *ThreadsDetail) AllStatus() []uint32 {
	return td.status
}

func (td *ThreadsDetail) AllCounter() []uint64 {
	return td.counter
}

func NewThreadsDetail(total uint64) *ThreadsDetail {
	td := &ThreadsDetail{
		status:  make([]uint32, total),
		counter: make([]uint64, total),
	}
	return td
}
