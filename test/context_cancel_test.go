package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

func TestContextCancelInPause(t *testing.T) {
	mt := Multitasking.NewMultitasking[int, int]("ContextCancelTest", nil)

	totalTasks := 10
	interruptedCount := 0
	var mu sync.Mutex

	mt.SetOnTerminating(func(c Multitasking.Controller[int, int], task int) {
		mu.Lock()
		interruptedCount++
		mu.Unlock()
	})

	mt.Register(func(dc Multitasking.DistributeController[int, int]) {
		for i := 0; i < totalTasks; i++ {
			dc.AddTask(i)
		}
	}, func(ec Multitasking.ExecuteController[int, int], tc Multitasking.ThreadController, data int) Multitasking.Result[int, int] {
		// 暂停系统
		ec.Pause()
		return ec.Success(data)
	})

	ctx, cancel := context.WithCancel(context.Background())
	
	go func() {
		// 等待系统处理第一个任务并进入暂停
		time.Sleep(100 * time.Millisecond)
		// 直接取消 Context，不调用 Terminate()
		cancel()
	}()

	results, err := mt.Run(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	totalProcessed := len(results) + interruptedCount
	t.Logf("Results: %d, Interrupted: %d, TotalProcessed: %d", len(results), interruptedCount, totalProcessed)

	// 只要没有死锁且任务被保存，就说明工作正常
	if totalProcessed == 0 {
		t.Error("Expected some tasks to be processed or saved, but got 0")
	}
}
