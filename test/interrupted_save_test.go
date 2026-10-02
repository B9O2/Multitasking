package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

func TestInterruptedSave(t *testing.T) {
	mt := Multitasking.NewMultitasking[int, int]("InterruptedSaveTest", nil)

	totalTasks := 100
	attemptedTasks := 0
	fromDC := 0
	fromEC := 0
	var mu sync.Mutex

	mt.SetOnTerminating(func(c Multitasking.Controller[int, int], task int) {
		fmt.Println("SAVED TASK")
		mu.Lock()
		defer mu.Unlock()
		switch c.(type) {
		case Multitasking.DistributeController[int, int]:
			fromDC++
		case Multitasking.ExecuteController[int, int]:
			fromEC++
		}
	})

	mt.Register(func(dc Multitasking.DistributeController[int, int]) {
		for i := 0; i < totalTasks; i++ {
			mu.Lock()
			attemptedTasks++ // 在 Add 之前计数，代表“离源”
			mu.Unlock()
			dc.AddTask(i)
		}
	}, func(ec Multitasking.ExecuteController[int, int], tc Multitasking.ThreadController, data int) Multitasking.Result[int, int] {
		time.Sleep(20 * time.Millisecond) // 增加耗时确保管道内有积压
		return ec.Success(data)
	})

	go func() {
		time.Sleep(50 * time.Millisecond)
		mt.Terminate()
	}()

	results, err := mt.Run(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	totalSaved := fromDC + fromEC
	totalProcessed := len(results) + totalSaved

	t.Logf(
		"Attempts: %d, Results: %d, SavedFromDC: %d, SavedFromEC: %d, TotalProcessed: %d",
		attemptedTasks,
		len(results),
		fromDC,
		fromEC,
		totalProcessed,
	)

	// 核心断言：尝试分发的每一个任务都必须有去处
	if totalProcessed != attemptedTasks {
		t.Errorf(
			"Data loss detected! Attempted: %d, but only accounted for: %d",
			attemptedTasks,
			totalProcessed,
		)
	}

	// 职责断言：必须有任务是从分发器拦截的 (fromDC)
	if fromDC != 1 {
		t.Errorf("Expected exactly 1 task intercepted by DC, got %d", fromDC)
	}

	// 职责断言：必须有任务是从管道收割的 (fromEC)
	if fromEC == 0 {
		t.Error("Expected some tasks to be saved from pipeline (EC), but got 0")
	}
}

func TestInterruptedSaveContextCancel(t *testing.T) {
	mt := Multitasking.NewMultitasking[int, int]("InterruptedSaveContextCancelTest", nil)

	totalTasks := 100
	attemptedTasks := 0
	fromDC := 0
	fromEC := 0
	var mu sync.Mutex

	mt.SetOnTerminating(func(c Multitasking.Controller[int, int], task int) {
		fmt.Println("SAVED TASK (Ctx Cancel)")
		mu.Lock()
		defer mu.Unlock()
		switch c.(type) {
		case Multitasking.DistributeController[int, int]:
			fromDC++
		case Multitasking.ExecuteController[int, int]:
			fromEC++
		}
	})

	mt.Register(func(dc Multitasking.DistributeController[int, int]) {
		for i := 0; i < totalTasks; i++ {
			mu.Lock()
			attemptedTasks++ // 在 Add 之前计数，代表“离源”
			mu.Unlock()
			dc.AddTask(i)
		}
	}, func(ec Multitasking.ExecuteController[int, int], tc Multitasking.ThreadController, data int) Multitasking.Result[int, int] {
		time.Sleep(20 * time.Millisecond) // 增加耗时确保管道内有积压
		return ec.Success(data)
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	results, err := mt.Run(ctx, 2)
	if err != nil {
		t.Logf("Run error (expected maybe): %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	totalSaved := fromDC + fromEC
	totalProcessed := len(results) + totalSaved

	t.Logf(
		"Attempts: %d, Results: %d, SavedFromDC: %d, SavedFromEC: %d, TotalProcessed: %d",
		attemptedTasks,
		len(results),
		fromDC,
		fromEC,
		totalProcessed,
	)

	// 核心断言：尝试分发的每一个任务都必须有去处
	if totalProcessed != attemptedTasks {
		t.Errorf(
			"Data loss detected! Attempted: %d, but only accounted for: %d",
			attemptedTasks,
			totalProcessed,
		)
	}

	// 职责断言：必须有任务是从分发器拦截的 (fromDC)
	if fromDC != 1 {
		t.Errorf("Expected exactly 1 task intercepted by DC, got %d", fromDC)
	}

	// 职责断言：必须有任务是从管道收割的 (fromEC)
	if fromEC == 0 {
		t.Error("Expected some tasks to be saved from pipeline (EC), but got 0")
	}
}
