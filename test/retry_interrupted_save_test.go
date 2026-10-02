package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

func TestRetryInterruptedSaveDeadlock(t *testing.T) {
	mt := Multitasking.NewMultitasking[int, int]("TestDeadlock", nil)

	mt.SetOnTerminating(func(c Multitasking.Controller[int, int], task int) {
		fmt.Println("SAVED TASK", task)
	})

	mt.Register(func(dc Multitasking.DistributeController[int, int]) {
		dc.AddTask(1)
	}, func(ec Multitasking.ExecuteController[int, int], tc Multitasking.ThreadController, data int) Multitasking.Result[int, int] {
		time.Sleep(20 * time.Millisecond)
		// return a retry result to put it in the priority queue
		// If we cancel the context right after, it might be stuck in retryQueue
		return ec.Retry(data + 1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond) // enough time for the task to finish execution and become a retry
		cancel()
	}()

	fmt.Println("Calling Run")
	done := make(chan struct{})
	go func() {
		mt.Run(ctx, 1)
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("Run returned successfully")
	case <-time.After(2 * time.Second):
		t.Fatal("DEADLOCK DETECTED! Run did not return")
	}
}
