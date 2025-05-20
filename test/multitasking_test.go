package test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

type Task struct {
	A, B, I int
}

var m = map[int]bool{}

func FastTasks(dc Multitasking.DistributeController) {
	//dc.Debug(true)
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
	for i := 0; i < 1000; i++ {
		dc.AddTask(Task{
			A: rand.Int(),
			B: rand.Int(),
			I: i,
		})
		final += 1
	}
}

func GenNumbersTerminate(dc Multitasking.DistributeController) {
	dc.Debug(true)
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
		fmt.Println("retry ", task.B)
		task.B += 1
		fmt.Println("new ", task.B)
		return ec.Retry(task)
	}
}

func HandleNumber(ec Multitasking.ExecuteController, i interface{}) interface{} {
	task := i.(Task)
	//mt.Log(1, "测试日志"+time.Now().String())
	ec.Protect(func() {
		m[task.A+task.B] = false
	})

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
			name:         "Single",
			distribution: GenNumbers,
			exec:         AddNumber,
			threads:      1,
		},
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
					ec.Terminate()
					return i, nil
				}),
			},
			threads: 20,
		},
		{
			name:         "Slow Middleware",
			distribution: GenNumbers,
			exec:         RetryNumber,
			middlewares: []Multitasking.Middleware{
				Multitasking.NewBaseMiddleware(func(ec Multitasking.ExecuteController, i interface{}) (interface{}, error) {
					time.Sleep(1 * time.Second)
					return i, nil
				}),
			},
			threads: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Println("TESTING [", tt.name, "]")
			mt.Register(tt.distribution, tt.exec)
			mt.SetResultMiddlewares(tt.middlewares...)
			mt.SetErrorCallback(func(c Multitasking.Controller, err error) {
				fmt.Println(reflect.TypeOf(c).String(), err)
			})
			fmt.Println("开始运行")
			res, err := mt.Run(context.Background(), tt.threads)
			fmt.Println("运行结束")
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
	_ = runtime.Stack(goroutines, true)

	finishRoutine := runtime.NumGoroutine()
	fmt.Printf("Total goroutines: %d\n", finishRoutine)
	//fmt.Println(string(goroutines[:length]))
	if baseRoutine != finishRoutine {
		panic(fmt.Sprintf("Routine Error:%d->%d", baseRoutine, finishRoutine))
	} else {
		fmt.Println("Routines OK")
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
func TestExternalTerminate(t *testing.T) {
	baseRoutine := runtime.NumGoroutine()
	mt := Multitasking.NewMultitasking("ovo", nil)
	mt.Register(GenNumbers, HandleNumber)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(100 * time.Microsecond)
		//mt.Terminate()
		fmt.Println("执行Cancel")
		cancel()
	}()

	mt.SetErrorCallback(func(ctrl Multitasking.Controller, err error) {
		switch ctrl.(type) {
		case Multitasking.ExecuteController:
			fmt.Println("Execute:", err)
		case Multitasking.DistributeController:
			fmt.Println("Distribute:", err)
		default:
			fmt.Println("Unknown Controller:", err)
		}
	})

	fmt.Println(mt)
	run, err := mt.Run(ctx, 100)
	if err != nil {
		return
	}
	fmt.Println(run)
	for _, event := range mt.Events(-2) {
		fmt.Println(event)
	}

	goroutines := make([]byte, 1<<20)
	_ = runtime.Stack(goroutines, true)

	finishRoutine := runtime.NumGoroutine()
	fmt.Printf("Total goroutines: %d\n", finishRoutine)
	//fmt.Println(string(goroutines[:length]))
	if baseRoutine != finishRoutine {
		panic(fmt.Sprintf("Routine Error:%d->%d", baseRoutine, finishRoutine))
	} else {
		fmt.Println("Routines OK")
	}

	/*
		for _, event := range mt.Events(1) {
			fmt.Println(event)
		}
	*/
}

type TerminateEC struct {
	*Multitasking.BaseExecuteController
	n int
}

func (tec *TerminateEC) T() {
	if tec.n > 100 {
		fmt.Println("###TERMINATE###")
		tec.Terminate()
	} else {
		tec.n += 1
	}

}

func NewTerminateEC() *TerminateEC {
	return &TerminateEC{
		Multitasking.NewBaseExecuteController(),
		0,
	}
}

func TestControllerTerminate(t *testing.T) {
	mt := Multitasking.NewMultitasking("Test", nil)
	mt.SetErrorCallback(func(c Multitasking.Controller, err error) {
		fmt.Println(reflect.TypeOf(c), err)
	})
	mt.SetController(NewTerminateEC())
	mt.Register(GenNumbers, func(ec Multitasking.ExecuteController, i any) any {
		task := i.(Task)
		//fmt.Println(task)
		if task.I > 2000 {
			ec.(*TerminateEC).T()
			return ec.Retry()
		}
		return 333
	})
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

func TestPause(t *testing.T) {
	baseRoutine := runtime.NumGoroutine()
	mt := Multitasking.NewMultitasking("ovo", nil)
	mt.Register(func(dc Multitasking.DistributeController) {
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
	}, func(ec Multitasking.ExecuteController, i interface{}) interface{} {
		task := i.(Task)
		//mt.Log(1, "测试日志"+time.Now().String())
		if task.I == 2000 {
			fmt.Println(">>>尝试暂停")
			ec.Pause()
			fmt.Println(">>>已暂停")
			time.Sleep(3 * time.Second)
			fmt.Println(">>>尝试恢复")
			ec.Resume()
			fmt.Println(">>>已恢复")
		}
		return task.A + task.B
	})
	mt.SetResultMiddlewares(Multitasking.NewBaseMiddleware(func(ec Multitasking.ExecuteController, i interface{}) (interface{}, error) {
		fmt.Println("Running...")

		return nil, nil
	}))
	mt.SetErrorCallback(func(ctrl Multitasking.Controller, err error) {
		switch ctrl.(type) {
		case Multitasking.ExecuteController:
			fmt.Println("Execute:", err)
		case Multitasking.DistributeController:
			fmt.Println("Distribute:", err)
		default:
			fmt.Println("Unknown Controller:", err)
		}
	})

	fmt.Println(mt)
	run, err := mt.Run(context.Background(), 100)
	if err != nil {
		return
	}
	fmt.Println(run)
	for _, event := range mt.Events(-2) {
		fmt.Println(event)
	}

	goroutines := make([]byte, 1<<20)
	_ = runtime.Stack(goroutines, true)

	finishRoutine := runtime.NumGoroutine()
	fmt.Printf("Total goroutines: %d\n", finishRoutine)
	//fmt.Println(string(goroutines[:length]))
	if baseRoutine != finishRoutine {
		panic(fmt.Sprintf("Routine Error:%d->%d", baseRoutine, finishRoutine))
	} else {
		fmt.Println("Routines OK")
	}

	/*
		for _, event := range mt.Events(1) {
			fmt.Println(event)
		}
	*/
}
