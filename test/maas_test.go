package test

import (
	"math/rand/v2"
	"testing"

	"github.com/B9O2/Multitasking"
	"github.com/rs/zerolog"
)

func TestMaaS(t *testing.T) {
	server := Multitasking.NewMultitasking[int]("MaaS", nil)
	server.Register(func(dc Multitasking.DistributeController[int]) {
		for {
			dc.AddTask(rand.Int())
		}
	}, func(ec Multitasking.ExecuteController[int], l zerolog.Logger, task int) int {
		if task%2 == 0 {
			return task * 2
		}
		return task * 3
	})
}
