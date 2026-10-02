package test

import (
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

func waitProcessed(pq *Multitasking.PriorityQueue[int]) {
	// 等待数据从 In 转移到堆
	for i := 0; i < 100; i++ {
		if len(pq.In) == 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func TestPriorityQueue_Basic(t *testing.T) {
	pq := Multitasking.NewPriorityQueue(func(a, b int) bool {
		return a < b // 数值小的优先级高
	})

	pq.In <- 10
	pq.In <- 5
	pq.In <- 20

	waitProcessed(pq)

	if pq.Len() != 3 {
		t.Errorf("expected length 3, got %d", pq.Len())
	}

	results := make([]int, 0, 3)
	for i := 0; i < 3; i++ {
		select {
		case val := <-pq.Out:
			results = append(results, val)
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout at index %d", i)
		}
	}

	expected := []int{5, 10, 20}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("at %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestPriorityQueue_Stability(t *testing.T) {
	type Task struct {
		Priority int
		ID       int
	}

	pq := Multitasking.NewPriorityQueue(func(a, b Task) bool {
		return a.Priority < b.Priority
	})

	pq.In <- Task{Priority: 1, ID: 1}
	pq.In <- Task{Priority: 2, ID: 2}
	pq.In <- Task{Priority: 1, ID: 3}
	pq.In <- Task{Priority: 1, ID: 4}

	expectedOrders := []int{1, 3, 4, 2}
	for _, expectedID := range expectedOrders {
		select {
		case task := <-pq.Out:
			if task.ID != expectedID {
				t.Errorf("expected ID %d, got %v", expectedID, task.ID)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for ID %d", expectedID)
		}
	}
}

func TestPriorityQueue_Close(t *testing.T) {
	pq := Multitasking.NewPriorityQueue(func(a, b int) bool {
		return a < b
	})

	pq.In <- 10
	pq.In <- 5
	close(pq.In)

	results := make([]int, 0)
	timeout := time.After(2 * time.Second)
	done := false
	for !done {
		select {
		case val, ok := <-pq.Out:
			if !ok {
				done = true
			} else {
				results = append(results, val)
			}
		case <-timeout:
			t.Fatal("timeout waiting for Out to close")
		}
	}

	if len(results) != 2 || results[0] != 5 || results[1] != 10 {
		t.Errorf("unexpected results: %v", results)
	}
}
