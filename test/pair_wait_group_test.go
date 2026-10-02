package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

func TestPairWaitGroup(t *testing.T) {
	// 初始化 PairWaitGroup 和两个 Handler
	pwg, hA, hB := Multitasking.NewPairWaitGroup()

	// 模拟级联任务流程：
	// 1. A 执行 2 个任务
	// 2. 任务完成后，各产生 1 个 B 任务 (共 2 个)
	// 3. B 任务完成后，各产生 1 个 A 任务 (共 2 个)
	// 4. 最后这 2 个 A 任务完成后，PairWaitGroup 应该能退出。

	hA.Add(2)

	// 模拟处理最初的 2 个 A 任务
	go func() {
		for i := 0; i < 2; i++ {
			time.Sleep(50 * time.Millisecond)
			fmt.Printf("[Worker A] Task %d done, Adding B task\n", i)
			hB.Add(1)
			hA.Done()
		}
	}()

	// 模拟处理 2 个 B 任务
	go func() {
		// 为了简化，我们等待 B 有任务了再处理
		for i := 0; i < 2; i++ {
			time.Sleep(100 * time.Millisecond) // 模拟处理耗时
			fmt.Printf("[Worker B] Task %d done, Adding A task\n", i)
			hA.Add(1)
			hB.Done()
		}
	}()

	// 模拟处理 B 产生的最后 2 个 A 任务
	go func() {
		// 延迟等待 B 产生任务
		time.Sleep(300 * time.Millisecond)
		for i := 0; i < 2; i++ {
			time.Sleep(50 * time.Millisecond)
			fmt.Printf("[Worker A Final] Task %d done\n", i)
			hB.Done()
		}
	}()

	done := make(chan struct{})
	go func() {
		pwg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("PairWaitGroup Wait() finished correctly as expected.")
	case <-time.After(2 * time.Second):
		t.Fatal("PairWaitGroup Wait() timed out! Check for deadlocks or logic errors.")
	}

	// 验证 Handler 的 closeChan 是否正确关闭
	select {
	case <-hA.Closed():
	default:
		t.Error("handlerA.closeChan was not closed after Wait()")
	}

	select {
	case <-hB.Closed():
	default:
		t.Error("handlerB.closeChan was not closed after Wait()")
	}
}

func TestPairWaitGroupSimple(t *testing.T) {
	pwg, hA, hB := Multitasking.NewPairWaitGroup()

	hA.Add(1)
	hB.Add(1)

	go func() {
		time.Sleep(50 * time.Millisecond)
		hA.Done()
		time.Sleep(50 * time.Millisecond)
		hB.Done()
	}()

	pwg.Wait()

	// 通道应该已经关闭
	_, okA := <-hA.Closed()
	if okA {
		t.Error("Expected channel A to be closed")
	}
	_, okB := <-hB.Closed()
	if okB {
		t.Error("Expected channel B to be closed")
	}
}

func TestPairWaitGroupPingPong(t *testing.T) {
	pwg, hA, hB := Multitasking.NewPairWaitGroup()

	const maxRounds = 5

	// A 的处理逻辑：处理 B 给的任务 (hB.Add)
	go func() {
		round := 0
		for {
			select {
			case <-hA.Closed():
				return
			default:
				// 如果 B 给 A 布置了任务 (通过 hB.Add)
				if hB.GetCount() > 0 {
					time.Sleep(10 * time.Millisecond)
					// fmt.Printf("[Worker A] Processing B's assignment, round %d\n", round)

					// 如果还没到最大轮数，A 处理完后，反手给 B 布置一个新任务 (hA.Add)
					if round < maxRounds {
						hA.Add(1)
					}

					hA.Done() // A 完成了 B 布置的一个任务
					round++
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// B 的处理逻辑：处理 A 给的任务 (hA.Add)
	go func() {
		round := 0
		for {
			select {
			case <-hB.Closed():
				return
			default:
				// 如果 A 给 B 布置了任务 (通过 hA.Add)
				if hA.GetCount() > 0 {
					time.Sleep(15 * time.Millisecond)
					// fmt.Printf("[Worker B] Processing A's assignment, round %d\n", round)

					// B 处理完后，反手再给 A 布置一个任务 (hB.Add)
					hB.Add(1)

					hB.Done() // B 完成了 A 布置的一个任务
					round++
				}
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	// 启动：A 先给 B 布置 1 个任务
	fmt.Println("=== Starting Ping-Pong (A -> B) ===")
	hA.Add(1)

	done := make(chan struct{})
	go func() {
		pwg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("=== Ping-Pong Finished Successfully ===")
	case <-time.After(3 * time.Second):
		t.Fatal("Ping-Pong test timed out! Possible deadlock or non-convergence.")
	}
}
