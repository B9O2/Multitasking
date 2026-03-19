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
	}, func(ec Multitasking.ExecuteController[int, int], data int) Multitasking.Result[int, int] {
		tid := ec.ThreadID()

		if tid < 0 || tid >= int64(numThreads) {
			t.Errorf("Unexpected ThreadID: %d", tid)
		}

		mu.Lock()
		threadIDs[tid] = true
		mu.Unlock()

		l := ec.Logger()
		l.Debug().Int("data", data).Msg("processing")

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
	}, func(ec Multitasking.ExecuteController[int, int], data int) Multitasking.Result[int, int] {
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

// CustomEC 定义一个用户自定义的执行控制器
type CustomEC[T any, R any] struct {
	*Multitasking.StandardExecuteController[T, R]
	CustomTag string
}

func (c *CustomEC[T, R]) WithContext(
	ctx context.Context,
) Multitasking.ExecuteController[T, R] {
	base := c.StandardExecuteController.WithContext(ctx)
	return &CustomEC[T, R]{
		StandardExecuteController: base.(*Multitasking.StandardExecuteController[T, R]),
		CustomTag:                 c.CustomTag,
	}
}

func (c *CustomEC[T, R]) WithLogger(
	logger zerolog.Logger,
) Multitasking.ExecuteController[T, R] {
	base := c.StandardExecuteController.WithLogger(logger)
	return &CustomEC[T, R]{
		StandardExecuteController: base.(*Multitasking.StandardExecuteController[T, R]),
		CustomTag:                 c.CustomTag,
	}
}

func TestCustomController(t *testing.T) {
	numThreads := uint64(5)
	mt := Multitasking.NewMultitasking[int, int]("TestCustomController", nil)

	myEC := &CustomEC[int, int]{
		StandardExecuteController: Multitasking.NewStandardExecuteController[int, int](),
		CustomTag:                 "SpecialWorker",
	}
	mt.SetController(myEC)

	var mu sync.Mutex
	tagCorrect := true
	recordedIDs := make(map[int64]bool)

	mt.Register(func(dc Multitasking.DistributeController[int, int]) {
		for i := 0; i < 50; i++ {
			dc.AddTask(i)
		}
	}, func(ec Multitasking.ExecuteController[int, int], data int) Multitasking.Result[int, int] {
		custom, ok := ec.(*CustomEC[int, int])

		tid := ec.ThreadID()

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

		l := ec.Logger()
		l.Info().Msg("custom log")

		return ec.Success(data)
	})

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
