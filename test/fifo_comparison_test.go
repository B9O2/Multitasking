package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
	"github.com/smallnest/chanx"
)

func TestFIFOConsistency(t *testing.T) {
	const count = 50000
	
	ch := chanx.NewUnboundedChan[int](context.Background(), 100)
	go func() {
		for i := 0; i < count; i++ {
			ch.In <- i
		}
		close(ch.In)
	}()

	chanxResults := make([]int, 0, count)
	for v := range ch.Out {
		chanxResults = append(chanxResults, v)
	}

	pq := Multitasking.NewPriorityQueue[int](nil)
	go func() {
		for i := 0; i < count; i++ {
			pq.In <- i
		}
		close(pq.In)
	}()

	pqResults := make([]int, 0, count)
	timeout := time.After(5 * time.Second)
	done := false
	for !done {
		select {
		case v, ok := <-pq.Out:
			if !ok {
				done = true
			} else {
				pqResults = append(pqResults, v)
			}
		case <-timeout:
			t.Errorf("PriorityQueue stalled! Collected %d/%d", len(pqResults), count)
			done = true
		}
	}

	if len(pqResults) != count {
		t.Fatalf("Expected %d results, got %d", count, len(pqResults))
	}

	for i := 0; i < count; i++ {
		if chanxResults[i] != i {
			t.Fatalf("chanx failed at index %d", i)
		}
		if pqResults[i] != i {
			t.Fatalf("PQ failed at index %d: exp %d, got %d", i, i, pqResults[i])
		}
	}
	fmt.Printf("FIFO Consistency check passed for %d items\n", count)
}

func TestConcurrentSafety(t *testing.T) {
	const workers = 10
	const itemsPerWorker = 5000
	const total = workers * itemsPerWorker

	pq := Multitasking.NewPriorityQueue[int](nil)
	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(base int) {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				pq.In <- base + i
			}
		}(w * 1000000)
	}

	collected := 0
	stop := make(chan struct{})
	go func() {
		defer close(stop)
		for range pq.Out {
			collected++
			if collected == total {
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(pq.In)
	}()

	select {
	case <-stop:
		if collected == total {
			fmt.Println("Concurrency safety check passed")
		} else {
			t.Errorf("Collected only %d/%d", collected, total)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("Concurrency test timed out! Collected %d/%d", collected, total)
	}
}
