package main

import (
	"context"
	"fmt"
	"github.com/B9O2/Multitasking"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"
)

type Task struct {
	A, B, I int
}

var m = map[int]bool{}

func FastTasks(dc Multitasking.DistributeController) {
	for i := 0; i < 40; i++ {
		dc.AddTask(Task{
			A: rand.Int(),
			B: rand.Int(),
		})
	}
}

func GenNumbers(dc Multitasking.DistributeController) {
	//dc.Debug(true)
	final := 0
	for i := 0; i < 10000; i++ {
		dc.AddTask(Task{
			A: rand.Int(),
			B: rand.Int(),
			I: i,
		})
		final += 1
	}
}

func GenNumbersTerminate(dc Multitasking.DistributeController) {
	//dc.Debug(true)
	final := 0
	for i := 0; i < 10000; i++ {
		dc.AddTask(Task{
			A: rand.Int(),
			B: rand.Int(),
			I: i,
		})
		if i > 1000 {
			dc.Terminate()
		}
		final += 1
	}
}

func AddNumber(ec Multitasking.ExecuteController, i interface{}) interface{} {
	task := i.(Task)
	return task.A + task.B
}

func RetryNumber(ec Multitasking.ExecuteController, i interface{}) interface{} {
	task := i.(Task)
	//mt.Log(1, "测试日志"+time.Now().String())
	ec.Protect(func() {
		m[task.A+task.B] = false
	})

	q := task.A + task.B
	if q%2 == 0 {
		return q
	} else {
		task.B += 1
		return ec.Retry(task)
	}
}

func HandleNumber(ec Multitasking.ExecuteController, i interface{}) interface{} {
	task := i.(Task)
	//mt.Log(1, "测试日志"+time.Now().String())
	ec.Protect(func() {
		m[task.A+task.B] = false
	})
	if task.I > 1000 {
		ec.Terminate()
	}
	return task.A + task.B
}

func TestMultitasking(t *testing.T) {
	baseRoutine := runtime.NumGoroutine()
	mt := Multitasking.NewMultitasking("Test", nil)
	tests := []struct {
		name         string
		distribution func(dc Multitasking.DistributeController)
		exec         func(ec Multitasking.ExecuteController, i interface{}) interface{}
		middlewares  []Multitasking.Middleware
		threads      uint
	}{
		{
			name:         "Normal",
			distribution: GenNumbers,
			exec:         AddNumber,
			threads:      200,
		},
		{
			name:         "Fast Terminate",
			distribution: FastTasks,
			exec:         AddNumber,
			middlewares: []Multitasking.Middleware{
				Multitasking.NewBaseMiddleware(func(ec Multitasking.ExecuteController, i interface{}) (interface{}, error) {
					ec.Terminate()
					return i, nil
				}),
			},
			threads: 20,
		},
		{
			name:         "Retry",
			distribution: GenNumbers,
			exec:         RetryNumber,
			middlewares: []Multitasking.Middleware{
				Multitasking.NewBaseMiddleware(func(ec Multitasking.ExecuteController, i interface{}) (interface{}, error) {
					//ec.Terminate()
					return i, nil
				}),
			},
			threads: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mt.Register(tt.distribution, tt.exec)
			mt.SetResultMiddlewares(tt.middlewares...)
			res, err := mt.Run(context.Background(), tt.threads)
			if err != nil {
				panic(err)
			}
			fmt.Println(res)
			fmt.Println(mt)
			//buf := make([]byte, 10240)
			//n := runtime.Stack(buf, true)
			//fmt.Println(string(buf[:n]))
		})
	}
	goroutines := make([]byte, 1<<20)
	length := runtime.Stack(goroutines, true)

	finishRoutine := runtime.NumGoroutine()
	fmt.Printf("Total goroutines: %d\n", finishRoutine)
	fmt.Println(string(goroutines[:length]))
	if baseRoutine != finishRoutine {
		panic(fmt.Sprintf("Routine Error:%d->%d", baseRoutine, finishRoutine))
	}
}

// ------------------------------示例-----------------------------------------//

func TestMultitaskingContext(t *testing.T) {
	mt := Multitasking.NewMultitasking("Test", nil)
	mt.Register(GenNumbers, HandleNumber)
	mt.SetResultMiddlewares()
	_, err := mt.Run(context.Background(), 200)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(mt)
	for _, event := range mt.Events(-2) {
		fmt.Println(event)
	}
}

// -----------------------------------------------------------------//
func TestRetry(t *testing.T) {
	rand.Seed(time.Now().Unix())
	type Task struct {
		A, B int
	}
	mt := Multitasking.NewMultitasking("retry Test", nil)
	fmt.Println(mt)
	mt.Register(func(dc Multitasking.DistributeController) {
		for i := 0; i < 1000; i++ {
			dc.AddTask(Task{
				A: rand.Int(),
				B: rand.Int(),
			})
		}
	}, func(ec Multitasking.ExecuteController, i interface{}) interface{} {
		task := i.(Task)
		switch i.(type) {
		case string:
			return 1004
		case Task:
			mt.Log(1, "测试日志"+time.Now().String())
			//time.Sleep(1 * time.Second)
			if (task.A+task.B)%2 == 0 {
				return task.A + task.B
			} else {
				return ec.Retry(Task{
					A: rand.Int(),
					B: rand.Int(),
				})
			}
		default:
			fmt.Println(reflect.TypeOf(i), i)
			return -1
		}
	})

	mt.SetErrorCallback(func(ctrl Multitasking.Controller, err error) interface{} {
		switch ctrl.(type) {
		case Multitasking.ExecuteController:
			fmt.Println("Execute:", err)
		case Multitasking.DistributeController:
			fmt.Println("Distribute:", err)
		default:
			fmt.Println("Unknown Controller:", err)
		}
		return nil
	})

	fmt.Println(mt)
	run, err := mt.Run(context.Background(), 100)
	if err != nil {
		return
	}

	fmt.Println(mt)
	for _, event := range mt.Events(-2) {
		fmt.Println(event)
	}

	fmt.Println(run)

	/*
		for _, event := range mt.Events(1) {
			fmt.Println(event)
		}
	*/
}
