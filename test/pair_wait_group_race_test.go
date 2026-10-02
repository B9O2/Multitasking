package test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

func TestPairWaitGroupRaceCondition(t *testing.T) {
	pwg, hA, hB := Multitasking.NewPairWaitGroup()

	const (
		numAssigners   = 10   // 派发任务的协程数
		tasksPerWorker = 500  // 每个协程派发的任务数 (减少总数以加快测试)
	)

	var wgAssigners sync.WaitGroup
	wgAssigners.Add(numAssigners * 2)

	// 模拟 A 端派发任务给 B (hA.Add 增加 hA.wg, 需要 hB.Done 来减少)
	for i := 0; i < numAssigners; i++ {
		go func() {
			defer wgAssigners.Done()
			for j := 0; j < tasksPerWorker; j++ {
				hA.Add(1)
				if j%50 == 0 {
					time.Sleep(time.Duration(rand.Intn(100)) * time.Nanosecond)
				}
			}
		}()
	}

	// 模拟 B 端派发任务给 A (hB.Add 增加 hB.wg, 需要 hA.Done 来减少)
	for i := 0; i < numAssigners; i++ {
		go func() {
			defer wgAssigners.Done()
			for j := 0; j < tasksPerWorker; j++ {
				hB.Add(1)
				if j%50 == 0 {
					time.Sleep(time.Duration(rand.Intn(100)) * time.Nanosecond)
				}
			}
		}()
	}

	// 模拟 A 处理 B 布置的任务 (hB.wg)
	// 使用互斥锁保证 Done 的调用与 GetCount 的原子性，防止 WaitGroup 变负数
	var muA sync.Mutex
	go func() {
		for {
			select {
			case <-hA.Closed():
				return
			default:
				muA.Lock()
				if hB.GetCount() > 0 {
					hA.Done()
				}
				muA.Unlock()
				time.Sleep(1 * time.Microsecond)
			}
		}
	}()

	// 模拟 B 处理 A 布置的任务 (hA.wg)
	var muB sync.Mutex
	go func() {
		for {
			select {
			case <-hB.Closed():
				return
			default:
				muB.Lock()
				if hA.GetCount() > 0 {
					hB.Done()
				}
				muB.Unlock()
				time.Sleep(1 * time.Microsecond)
			}
		}
	}()

	// 启动等待
	done := make(chan struct{})
	go func() {
		pwg.Wait()
		close(done)
	}()

	// 等待所有派发者完成
	wgAssigners.Wait()

	// 验证
	select {
	case <-done:
		// 成功退出
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout! Logic failed to converge. Counts: A=%d, B=%d", hA.GetCount(), hB.GetCount())
	}
}
