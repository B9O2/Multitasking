package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

func TestDebugInterruptibleTask(t *testing.T) {
	mt := Multitasking.NewMultitasking[int, int]("InterruptibleTask", nil)

	var mu sync.Mutex
	savedTasks := 0

	mt.SetOnTerminating(func(c Multitasking.Controller[int, int], task int) {
		mu.Lock()
		defer mu.Unlock()
		savedTasks++
		fmt.Printf("SAVED TASK: %d\n", task)
	})

	mt.Register(func(dc Multitasking.DistributeController[int, int]) {
		dc.AddTask(1)
	}, func(ec Multitasking.ExecuteController[int, int], tc Multitasking.ThreadController, data int) Multitasking.Result[int, int] {
		fmt.Println("Worker: Task 1 started...")
		select {
		case <-ec.Context().Done():
			fmt.Println("Worker: Task 1 aborted due to context cancellation!")
			time.Sleep(100 * time.Millisecond) // Ensure afterfunc runs
			return ec.Null()
		case <-time.After(5 * time.Second):
			return ec.Success(data)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(200 * time.Millisecond)
		fmt.Println(">>> TRIGGER CANCEL <<<")
		cancel()
	}()

	done := make(chan struct{})
	go func() {
		mt.Run(ctx, 1)
		close(done)
	}()

	<-done
	fmt.Printf("Total saved tasks: %d\n", savedTasks)
}
