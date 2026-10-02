package test

import (
	"context"
	"testing"
	"time"
	"github.com/B9O2/Multitasking"
)

func TestReproWaitGroupPanic(t *testing.T) {
	// 增加循环次数并添加重试机制
	for i := 0; i < 1000; i++ {
		mt := Multitasking.NewMultitasking[int, int]("ReproPanic", nil)
		
		mt.Register(func(dc Multitasking.DistributeController[int, int]) {
			// 模拟立即结束
		}, func(ec Multitasking.ExecuteController[int, int], tc Multitasking.ThreadController, task int) Multitasking.Result[int, int] {
			return nil
		})

		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Iteration %d failed: recovered from panic: %v", i, r)
				}
			}()
			
			// 为了增加竞态几率，我们可以在特定时间片运行
			_, _ = mt.Run(context.Background(), 1)
		}()
		
		// 每次运行后稍作停顿，让调度器有机会重排
		time.Sleep(time.Microsecond)
	}
}
