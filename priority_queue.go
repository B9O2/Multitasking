package Multitasking

import (
	"container/heap"
	"sync"
)

type priorityItem[T any] struct {
	value    T
	sequence uint64
}

type internalHeap[T any] struct {
	items []*priorityItem[T]
	less  func(a, b T) bool
}

func (h *internalHeap[T]) Len() int { return len(h.items) }

func (h *internalHeap[T]) Swap(
	i, j int,
) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *internalHeap[T]) Push(
	x any,
) {
	h.items = append(h.items, x.(*priorityItem[T]))
}
func (h *internalHeap[T]) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

func (h *internalHeap[T]) Less(i, j int) bool {
	if h.less != nil {
		if h.less(h.items[i].value, h.items[j].value) {
			return true
		}
		if h.less(h.items[j].value, h.items[i].value) {
			return false
		}
	}
	return h.items[i].sequence < h.items[j].sequence
}

type PriorityQueue[T any] struct {
	In  chan T
	Out chan T

	mu           sync.Mutex
	h            *internalHeap[T]
	nextSequence uint64
}

func NewPriorityQueue[T any](less func(a, b T) bool) *PriorityQueue[T] {
	pq := &PriorityQueue[T]{
		In:  make(chan T, 128),
		Out: make(chan T),
		h: &internalHeap[T]{
			less:  less,
			items: make([]*priorityItem[T], 0),
		},
	}
	go pq.run()
	return pq
}

func (pq *PriorityQueue[T]) run() {
	defer close(pq.Out)
	inChan := pq.In

	for {
		// 1. 优先级：尽可能先排空所有输入到堆中
	drainInput:
		for {
			select {
			case val, ok := <-inChan:
				if !ok {
					inChan = nil
					break drainInput
				}
				pq.pushInternal(val)
			default:
				break drainInput
			}
		}

		// 2. 检查堆
		pq.mu.Lock()
		if pq.h.Len() == 0 {
			pq.mu.Unlock()
			if inChan == nil {
				return
			}

			// 堆空且输入没关，阻塞等待
			val, ok := <-inChan
			if !ok {
				return
			}
			pq.pushInternal(val)
			continue
		}

		// 3. 有数据，尝试发送或接收
		topItem := pq.h.items[0]
		topValue := topItem.value
		pq.mu.Unlock()

		select {
		case val, ok := <-inChan:
			if !ok {
				inChan = nil
			} else {
				pq.pushInternal(val)
			}
			// 再次进入 drainInput 循环
		case pq.Out <- topValue:
			pq.mu.Lock()
			// 只有发送成功，且堆顶没变（或者还是同一个 sequence）才 Pop
			if pq.h.Len() > 0 && pq.h.items[0].sequence == topItem.sequence {
				heap.Pop(pq.h)
			}
			pq.mu.Unlock()
		}
	}
}

func (pq *PriorityQueue[T]) pushInternal(val T) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(pq.h, &priorityItem[T]{
		value:    val,
		sequence: pq.nextSequence,
	})
	pq.nextSequence++
}

func (pq *PriorityQueue[T]) BufLen() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.In) + pq.h.Len()
}

func (pq *PriorityQueue[T]) Len() int {
	return pq.BufLen()
}
