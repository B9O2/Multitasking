package test

import (
	"context"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

func TestPriorityRetrySimple(t *testing.T) {
	mt := Multitasking.NewMultitasking[int, int]("SimplePriorityTest", nil)

	mt.SetPriorityComparator(func(a, b int) bool {
		return a > b
	})

	mt.Register(func(dc Multitasking.DistributeController[int, int]) {
		dc.AddTasks(1, 2, 3)
	}, func(ec Multitasking.ExecuteController[int, int], tc Multitasking.ThreadController, data int) Multitasking.Result[int, int] {
		return ec.Success(data)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	results, err := mt.Run(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
}
