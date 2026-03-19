package test

import (
	"math/rand/v2"
	"testing"

	"github.com/B9O2/Multitasking"
)

func TestMaaS(t *testing.T) {
	server := Multitasking.NewMultitasking[int, int]("MaaS", nil)
	server.Register(func(dc Multitasking.DistributeController[int, int]) {
		for {
			dc.AddTask(rand.Int())
		}
	}, func(ec Multitasking.ExecuteController[int, int], task int) Multitasking.Result[int, int] {
		if task%2 == 0 {
			return ec.Success(task * 2)
		}
		return ec.Success(task * 3)
	})
}
