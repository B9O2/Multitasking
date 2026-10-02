package test

import (
	"testing"
	"time"
	"github.com/B9O2/Multitasking"
)

func TestWaiterPanic(t *testing.T) {
	w := Multitasking.NewWaiter()
	
	// 修正后的模式：先 Add 再启动协程
	w.Add(1)

	// 启动一个协程，立即完成工作并报告自己已等待结束
	go func() {
		// 模拟 SchedulingGate 的逻辑：
		// 首先标记它已完成（对应于 startSchedulingGate 看到 taskQueue 关闭）
		w.Done("SchedulingGate")
		
		// 接着进行 Wait（对应于 startDistribution 的 defer 逻辑）
		// 由于 SchedulingGate 已经 Done 了，Wait("SchedulingGate") 会立即返回并执行最后的 w.processWg.Done()
		w.Wait("SchedulingGate")
	}()

	// 故意停顿一下，确保协程中的逻辑在 WaitAll() 调用之前运行完
	time.Sleep(time.Millisecond * 50)

	// 此时 WaitAll 只是阻塞等待，不会再 Add，从而避免了竞态导致的负值计数器
	w.WaitAll()
}
