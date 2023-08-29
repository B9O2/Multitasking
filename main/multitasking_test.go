package main

import (
	"fmt"
	"github.com/B9O2/Multitasking"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var m = map[int]bool{}

func TestMultitasking(t *testing.T) {
	type Task struct {
		A, B int
	}
	mt := Multitasking.NewMultitasking("Test", 2)
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
		ec.Protect(func() error {
			m[task.A+task.B] = false
			return nil
		})
		return task.A + task.B
	})
	mt.SetErrorCallback(func(ec Multitasking.ExecuteController, err error) any {
		fmt.Println(err)
		return 1004
	})

	fmt.Println(mt)
	run, err := mt.Run()
	if err != nil {
		return
	}
	fmt.Println(mt)
	fmt.Println("Result: ", run)

	for _, event := range mt.Events(-2) {
		fmt.Println(event)
	}

}

func TestMultitaskingContext(t *testing.T) {
	type Task struct {
		A, B, I int
	}
	mt := Multitasking.NewMultitasking("Test", 400)
	fmt.Println(mt)
	mt.Register(func(dc Multitasking.DistributeController) {
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
		mt.Log(1, "Final: "+strconv.Itoa(final))
	}, func(ec Multitasking.ExecuteController, i interface{}) interface{} {
		task := i.(Task)
		//mt.Log(1, "测试日志"+time.Now().String())
		ec.Protect(func() error {
			m[task.A+task.B] = false
			return nil
		})
		if task.I > 1000 {
			ec.Terminate()
		}
		return task.A + task.B
	})
	mt.SetResultMiddlewares()

	fmt.Println(mt)

	_, err := mt.Run()
	if err != nil {
		return
	}

	fmt.Println(mt)
	for _, event := range mt.Events(-2) {
		fmt.Println(event)
	}
}

func TestRetry(t *testing.T) {
	rand.Seed(time.Now().Unix())
	type Task struct {
		A, B int
	}
	mt := Multitasking.NewMultitasking("retry Test", 100)
	fmt.Println(mt)
	mt.Register(func(dc Multitasking.DistributeController) {
		for i := 0; i < 10000; i++ {
			dc.AddTask(Task{
				A: rand.Int(),
				B: rand.Int(),
			})
		}
	}, func(ec Multitasking.ExecuteController, i interface{}) interface{} {
		switch i.(type) {
		case string:
			return 1004
		case Task:
			task := i.(Task)
			mt.Log(1, "测试日志"+time.Now().String())
			if (task.A+task.B)%2 != 0 {
				ec.Retry("RETRY_OK")
			}
			return task.A + task.B
		default:
			return -1
		}
	})
	mt.SetErrorCallback(func(ec Multitasking.ExecuteController, err error) interface{} {
		fmt.Println("EXEC:", err)
		return nil
	})

	fmt.Println(mt)
	_, err := mt.Run()
	if err != nil {
		return
	}

	fmt.Println(mt)
	for _, event := range mt.Events(-2) {
		fmt.Println(event)
	}
	//fmt.Println(run)

	/*
		for _, event := range mt.Events(1) {
			fmt.Println(event)
		}
	*/
}