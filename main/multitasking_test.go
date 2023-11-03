package main

import (
	"fmt"
	"github.com/B9O2/Multitasking"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

var m = map[int]bool{}

func TestMultitasking(t *testing.T) {
	type Task struct {
		A, B int
	}
	mt := Multitasking.NewMultitasking("Test", nil)
	fmt.Println(mt)
	mt.Register(func(dc Multitasking.DistributeController) {
		for i := 0; i < 4; i++ {
			dc.AddTask(Task{
				A: rand.Int(),
				B: rand.Int(),
			})
		}
	}, func(ec Multitasking.ExecuteController, i interface{}) interface{} {
		task := i.(Task)
		mt.Log(1, "测试日志"+time.Now().String())
		ec.Protect(func() {
			m[task.A+task.B] = false
		})
		return task.A + task.B
	})
	mt.SetErrorCallback(func(ec Multitasking.Controller, err error) any {
		fmt.Println(err)
		return 1004
	})

	fmt.Println(mt)
	run, err := mt.Run(1000)
	if err != nil {
		return
	}
	fmt.Println(mt)
	fmt.Println("Result: ", run)

	for _, event := range mt.Events(-2) {
		fmt.Println(event)
	}

}

// ------------------------------示例-----------------------------------------//
type Task struct {
	A, B, I int
}

func GenNumbers(dc Multitasking.DistributeController) {
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

func TestMultitaskingContext(t *testing.T) {
	mt := Multitasking.NewMultitasking("Test", nil)
	mt.Register(GenNumbers, HandleNumber)
	mt.SetResultMiddlewares()
	_, err := mt.Run(1000)
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
	run, err := mt.Run(100)
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
