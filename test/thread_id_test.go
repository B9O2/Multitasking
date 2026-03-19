package test

import (
	"context"
	"sync"
	"testing"

	"github.com/B9O2/Multitasking"
	"github.com/rs/zerolog"
)

func TestThreadID(t *testing.T) {
	numThreads := uint64(10)
	mt := Multitasking.NewMultitasking[int, int]("TestThreadID", nil)

	var mu sync.Mutex
	threadIDs := make(map[int64]bool)

	mt.Register(func(dc Multitasking.DistributeController[int, int]) {
		for i := 0; i < 100; i++ {
			dc.AddTask(i)
		}
	}, func(ec Multitasking.ExecuteController[int, int], logger zerolog.Logger, data int) Multitasking.Result[int, int] {
		tid := ec.ThreadID()

		if tid < 0 || tid >= int64(numThreads) {
			t.Errorf("Unexpected ThreadID: %d", tid)
		}

		mu.Lock()
		threadIDs[tid] = true
		mu.Unlock()

		return ec.Success(data)
	})

	_, err := mt.Run(context.Background(), numThreads)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if len(threadIDs) == 0 {
		t.Error("No ThreadIDs were recorded")
	}

	t.Logf("Total unique ThreadIDs recorded: %d", len(threadIDs))
	for tid := range threadIDs {
		if tid < 0 {
			t.Errorf("Found negative ThreadID: %d", tid)
		}
	}
}

func TestNoThreadID(t *testing.T) {
	mt := Multitasking.NewMultitasking[int, int]("TestNoThreadID", nil)
	var capturedEC Multitasking.ExecuteController[int, int]

	mt.Register(func(dc Multitasking.DistributeController[int, int]) {
		dc.AddTask(1)
	}, func(ec Multitasking.ExecuteController[int, int], logger zerolog.Logger, data int) Multitasking.Result[int, int] {
		capturedEC = ec
		return ec.Success(data)
	})

	_, _ = mt.Run(context.Background(), 1)

	if capturedEC == nil {
		t.Fatal("Failed to capture ExecuteController")
	}

	noIdEC := capturedEC.WithContext(context.Background())

	tid := noIdEC.ThreadID()
	if tid != -1 {
		t.Errorf("Expected -1 for missing ThreadID context, got %d", tid)
	}
}
