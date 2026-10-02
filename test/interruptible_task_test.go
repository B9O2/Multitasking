package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

func TestInterruptibleTaskIsSaved(t *testing.T) {
	mt := Multitasking.NewMultitasking[int, int]("InterruptibleTask", nil)

	var mu sync.Mutex
	savedTasks := 0

	// 注册断点续传钩子
	mt.SetOnTerminating(func(c Multitasking.Controller[int, int], task int) {
		mu.Lock()
		defer mu.Unlock()
		savedTasks++
		fmt.Printf("SAVED TASK: %d\n", task)
	})

	mt.Register(func(dc Multitasking.DistributeController[int, int]) {
		// 只下发 1 个任务
		dc.AddTask(1)
	}, func(ec Multitasking.ExecuteController[int, int], tc Multitasking.ThreadController, data int) Multitasking.Result[int, int] {
		fmt.Println("Worker: Task 1 started, simulating long calculation...")
		
		// 模拟一个既耗时又可以响应中断的业务逻辑
		select {
		case <-ec.Context().Done():
			// 业务主动响应中断，返回 ec.Null()
			fmt.Println("Worker: Task 1 aborted due to context cancellation!")
			time.Sleep(10 * time.Millisecond) // 等待框架的 terminating 状态同步
			return ec.Null()
		case <-time.After(5 * time.Second):
			// 正常跑完需要 5 秒
			fmt.Println("Worker: Task 1 finished successfully!")
			return ec.Success(data)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())

	// 在 200ms 的时候人为触发中断
	go func() {
		time.Sleep(200 * time.Millisecond)
		fmt.Println(">>> TRIGGER CANCEL <<<")
		cancel()
	}()

	done := make(chan struct{})
	go func() {
		fmt.Println("Calling mt.Run...")
		mt.Run(ctx, 1)
		fmt.Println("mt.Run returned!")
		close(done)
	}()

	select {
	case <-done:
		// 检查是否有任务被拯救
		mu.Lock()
		fmt.Printf("Total saved tasks: %d\n", savedTasks)
		mu.Unlock()
		
		if savedTasks != 1 {
			t.Errorf("Expected 1 saved task, but got %d", savedTasks)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Deadlock! mt.Run did not return.")
	}
}
