package test

import (
	"sync"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

// Helper: 辅助等待数据进入堆
func waitForHeap(pq *Multitasking.PriorityQueue[int]) {
	for i := 0; i < 50; i++ {
		if len(pq.In) == 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestPriorityQueue_Comprehensive(t *testing.T) {
	t.Run("BasicOrdering", func(t *testing.T) {
		pq := Multitasking.NewPriorityQueue(func(a, b int) bool {
			return a < b // 最小值优先
		})
		inputs := []int{10, 5, 20, 1, 15}
		for _, v := range inputs {
			pq.In <- v
		}
		
		expected := []int{1, 5, 10, 15, 20}
		for _, exp := range expected {
			select {
			case val := <-pq.Out:
				if val != exp {
					t.Errorf("expected %d, got %d", exp, val)
				}
			case <-time.After(1 * time.Second):
				t.Fatalf("timeout waiting for %d", exp)
			}
		}
	})

	t.Run("StabilityFIFO", func(t *testing.T) {
		type item struct {
			p int
			v string
		}
		pq := Multitasking.NewPriorityQueue(func(a, b item) bool {
			return a.p < b.p
		})

		// 相同优先级 p=1，按 A, B, C 顺序进入
		pq.In <- item{1, "A"}
		pq.In <- item{1, "B"}
		pq.In <- item{1, "C"}
		pq.In <- item{0, "First"} // 更高优先级

		expected := []string{"First", "A", "B", "C"}
		for _, exp := range expected {
			select {
			case val := <-pq.Out:
				if val.v != exp {
					t.Errorf("expected %s, got %s", exp, val.v)
				}
			case <-time.After(1 * time.Second):
				t.Fatal("timeout")
			}
		}
	})

	t.Run("CloseAndDrain", func(t *testing.T) {
		pq := Multitasking.NewPriorityQueue(func(a, b int) bool {
			return a < b
		})
		for i := 10; i > 0; i-- {
			pq.In <- i
		}
		close(pq.In)

		count := 0
		for val := range pq.Out {
			count++
			if val != count {
				t.Errorf("expected %d, got %d", count, val)
			}
		}
		if count != 10 {
			t.Errorf("expected 10 items, got %d", count)
		}
	})

	t.Run("NilComparator", func(t *testing.T) {
		// 之前触发 panic 的场景
		pq := Multitasking.NewPriorityQueue[int](nil)
		pq.In <- 100
		pq.In <- 50
		close(pq.In)

		// 应该按 FIFO 输出
		res1 := <-pq.Out
		res2 := <-pq.Out
		if res1 != 100 || res2 != 50 {
			t.Errorf("expected FIFO 100, 50; got %d, %d", res1, res2)
		}
	})

	t.Run("PreemptionLogic", func(t *testing.T) {
		// 测试“插队”：在 Out 没被读取时，注入更高优先级的元素
		pq := Multitasking.NewPriorityQueue(func(a, b int) bool {
			return a < b
		})

		pq.In <- 100 // 第一个进入，堆顶
		time.Sleep(20 * time.Millisecond) // 确保 run 协程已经 Pop 出了 100 并在 select 等待
		
		pq.In <- 10  // 更高优先级进入，应该让 100 归还堆并让 10 插队
		time.Sleep(20 * time.Millisecond)

		select {
		case val := <-pq.Out:
			if val != 10 {
				t.Errorf("expected 10 to preempt 100, but got %d", val)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("timeout")
		}
		
		if (<-pq.Out) != 100 {
			t.Error("100 should be the second one")
		}
	})

	t.Run("ConcurrencyAndBufLen", func(t *testing.T) {
		pq := Multitasking.NewPriorityQueue(func(a, b int) bool {
			return a < b
		})
		const numWriters = 10
		const itemsPerWriter = 100
		
		var wg sync.WaitGroup
		wg.Add(numWriters)
		for i := 0; i < numWriters; i++ {
			go func(base int) {
				defer wg.Done()
				for j := 0; j < itemsPerWriter; j++ {
					pq.In <- base + j
				}
			}(i * 1000)
		}

		wg.Wait()
		waitForHeap(pq)

		expectedTotal := numWriters * itemsPerWriter
		if pq.BufLen() != expectedTotal {
			t.Errorf("BufLen mismatch: expected %d, got %d", expectedTotal, pq.BufLen())
		}

		// 验证输出是否完全有序
		last := -1
		for i := 0; i < expectedTotal; i++ {
			val := <-pq.Out
			if val < last {
				t.Fatalf("out of order at index %d: %d follows %d", i, val, last)
			}
			last = val
		}
	})
}
