package Multitasking

import (
	"sync"
	"sync/atomic"
)

type Handler struct {
	wg, otherWG *CounterWaitGroup
	closeChan   chan struct{}
}

func (h *Handler) Add(delta int) {
	h.wg.Add(delta)
}

func (h *Handler) Done() {
	h.otherWG.Done()
}

func (h *Handler) GetCount() int32 {
	return h.wg.GetCount()
}

func (h *Handler) Closed() chan struct{} {
	return h.closeChan
}

func NewHandler() *Handler {
	return &Handler{
		wg:        NewCounterWaitGroup(),
		otherWG:   nil,
		closeChan: make(chan struct{}),
	}
}

// PairWaitGroup
type PairWaitGroup struct {
	handlerA, handlerB *Handler
}

func (pwg *PairWaitGroup) Wait() {
	for {
		if pwg.handlerA.GetCount() > 0 {
			pwg.handlerA.Wait()
		}
		if pwg.handlerB.GetCount() > 0 {
			pwg.handlerB.Wait()
		}
		if pwg.handlerA.GetCount()+pwg.handlerB.GetCount() == 0 {
			break
		}
	}
	close(pwg.handlerA.closeChan)
	close(pwg.handlerB.closeChan)
}

func NewPairWaitGroup() (*PairWaitGroup, *Handler, *Handler) {
	handlerA := NewHandler()
	handlerB := NewHandler()

	handlerA.otherWG = handlerB.wg
	handlerB.otherWG = handlerA.wg

	pwg := &PairWaitGroup{
		handlerA: handlerA,
		handlerB: handlerB,
	}

	return pwg, pwg.handlerA, pwg.handlerB
}

func (h *Handler) Wait() {
	h.wg.Wait()
}

// CounterWaitGroup 带有原子计数器的 WaitGroup
type CounterWaitGroup struct {
	wg    *sync.WaitGroup
	count atomic.Int32
	mu    sync.RWMutex
}

func (m *CounterWaitGroup) Add(delta int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.count.Add(int32(delta))
	m.wg.Add(delta)
}

func (m *CounterWaitGroup) Done() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.count.Add(-1)
	m.wg.Done()
}

func (m *CounterWaitGroup) GetCount() int32 {
	return m.count.Load()
}

func (m *CounterWaitGroup) Wait() {
	m.wg.Wait()
}

func NewCounterWaitGroup() *CounterWaitGroup {
	return &CounterWaitGroup{
		wg:    &sync.WaitGroup{},
		count: atomic.Int32{},
	}
}

// WaitGroupDone 将 WaitGroup 转为 chan
func WaitGroupDone(wg *sync.WaitGroup) chan struct{} {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}
