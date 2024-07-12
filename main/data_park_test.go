package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/B9O2/Multitasking"
)

func TestDataPark(t *testing.T) {
	dp := Multitasking.NewDataPark()

	v, ok := dp.Get("test")
	fmt.Println(0, ok, v)

	go func() {
		fmt.Println(1, "waiting")
		v := dp.Require("test")
		fmt.Println(1, v)
	}()

	go func() {
		fmt.Println(2, "waiting")
		v := dp.Require("test")
		fmt.Println(2, v)
	}()

	time.Sleep(3 * time.Second)
	dp.Put("test", "works!")

	go func() {
		fmt.Println(3, "waiting")
		v := dp.Require("test")
		fmt.Println(3, v)
	}()
	go func() {
		fmt.Println(4, "waiting")
		v := dp.Require("test")
		fmt.Println(4, v)
	}()

	v, ok = dp.Get("test")
	fmt.Println(5, ok, v)

time.Sleep(3 * time.Second)
	dp.Close()
}
