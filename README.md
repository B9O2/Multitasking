# Multitasking

多数并发需求都可以被分解为任务分发、任务执行与结果收集三部分，或多个这三部分的组合。Multitasking隐藏了其中线程调度的细节，并提供了任务重试、异常处理、执行中断和结果处理中间件等功能。

### Quick Start

示例：并发向一个网站发送多个请求

```go
package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/B9O2/Multitasking"
)

// 向example.com发送key
func Send(key string) int {
	resp, err := http.Get("https://www.example.com?key=" + key)
	if err != nil {
		fmt.Println("Error making GET request:", err)
		return -1
	}
	return resp.StatusCode
}

func main() {
	//实例化Multitasking
	mt := Multitasking.NewMultitasking("sender", nil)

	//注册任务分发与任务执行
	mt.Register(func(dc Multitasking.DistributeController) {
		/*
			任务分发负责将任务添加至任务队列，任务分发与任务执行会同时进行
		*/
		for _, key := range []string{"hello", "world"} {
			dc.AddTask(key)
		}
		/*
			ps. 这个示例中可以用更简单的dc.AddTasks()代替
		*/
	}, func(ec Multitasking.ExecuteController, key any) any {
		/*
			任务执行负责单个任务的执行逻辑，第二个入参是任务队列中的一个任务，返回值是任务执行的结果。
		*/
		return Send(key.(string))
	})

	//Run()会阻塞直到所有任务执行完毕或被中断(Terminate)
	results, err := mt.Run(context.Background(), 200) //以200个线程运行
	if err != nil {
		panic(err)
	}

	for _, result := range results {
		fmt.Println(result)
	}

}
```
