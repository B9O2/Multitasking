package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

func TestThreadID(t *testing.T) {
	numThreads := uint64(10)
	mt := Multitasking.NewMultitasking[int, int]("TestThreadID", nil)

	var mu sync.Mutex
	threadIDs := make(map[int64]bool)

	mt.Register(
		func(dc Multitasking.DistributeController[int, int], tc Multitasking.ThreadController) {
			for i := 0; i < 100; i++ {
				dc.AddTask(i)
			}
		},
		func(ec Multitasking.ExecuteController[int, int], tc Multitasking.ThreadController, data int) Multitasking.Result[int, int] {
			tid := tc.ThreadID()

			if tid < 0 || tid >= int64(numThreads) {
				t.Errorf("Unexpected ThreadID: %d", tid)
			}

			mu.Lock()
			threadIDs[tid] = true
			mu.Unlock()

			l := tc.Logger()
			l.Debug().Int("data", data).Msg("processing")

			return ec.Success(data)
		},
	)

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

type CustomEC[T any, R any] struct {
	*Multitasking.BaseExecuteController[T, R]
	CustomTag string
}

func TestCustomController(t *testing.T) {
	numThreads := uint64(5)
	mt := Multitasking.NewMultitasking[int, int]("TestCustomController", nil)

	myEC := &CustomEC[int, int]{
		BaseExecuteController: Multitasking.NewBaseExecuteController[int, int](),
		CustomTag:             "SpecialWorker",
	}
	mt.SetController(myEC)

	var mu sync.Mutex
	tagCorrect := true
	recordedIDs := make(map[int64]bool)

	mt.Register(
		func(dc Multitasking.DistributeController[int, int], tc Multitasking.ThreadController) {
			for i := 0; i < 50; i++ {
				dc.AddTask(i)
			}
		},
		func(ec Multitasking.ExecuteController[int, int], tc Multitasking.ThreadController, data int) Multitasking.Result[int, int] {
			// 类型断言依然有效
			custom, ok := ec.(*CustomEC[int, int])

			tid := tc.ThreadID()

			mu.Lock()
			defer mu.Unlock()

			if !ok || custom.CustomTag != "SpecialWorker" {
				tagCorrect = false
			}

			if tid < 0 || tid >= int64(numThreads) {
				t.Errorf("Unexpected ThreadID in custom controller: %d", tid)
			} else {
				recordedIDs[tid] = true
			}

			l := tc.Logger()
			l.Info().Msg("custom log")

			return ec.Success(data)
		},
	)

	_, err := mt.Run(context.Background(), numThreads)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if !tagCorrect {
		t.Error("Custom controller tag was lost or type assertion failed")
	}

	if len(recordedIDs) == 0 {
		t.Error("No ThreadIDs were recorded in custom controller")
	}

	t.Logf("Custom Controller unique ThreadIDs: %d", len(recordedIDs))
	if len(recordedIDs) != int(numThreads) {
		t.Errorf(
			"Expected %d unique ThreadIDs, but got %d. Some threads might not have been utilized or IDs were wrong.",
			numThreads,
			len(recordedIDs),
		)
	}
}

func TestThreadIsolationProof(t *testing.T) {
	numThreads := uint64(50)
	mt := Multitasking.NewMultitasking[int, int]("IsolationProof", nil)

	var mu sync.Mutex
	failed := false

	mt.Register(
		func(dc Multitasking.DistributeController[int, int], tc Multitasking.ThreadController) {
			for i := 0; i < 200; i++ {
				dc.AddTask(i)
			}
		},
		func(ec Multitasking.ExecuteController[int, int], tc Multitasking.ThreadController, data int) Multitasking.Result[int, int] {
			expectedID := tc.ThreadID()

			time.Sleep(10 * time.Millisecond)

			actualID := tc.ThreadID()

			if expectedID != actualID {
				mu.Lock()
				failed = true
				mu.Unlock()
			}

			return ec.Success(data)
		},
	)

	_, _ = mt.Run(context.Background(), numThreads)

	if failed {
		t.Error("PROVED: Thread identity was overwritten!")
	} else {
		t.Log("PROVED: Thread identity is perfectly isolated via tc.")
	}
}
