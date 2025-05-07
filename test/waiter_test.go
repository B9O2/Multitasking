package test

import (
	"fmt"
	"github.com/B9O2/Multitasking"
	"testing"
	"time"
)

func TestWaiter(t *testing.T) {
	w := Multitasking.NewWaiter()
	go func() {
		fmt.Println("[-] 1Seconds Running")
		time.Sleep(1 * time.Second)
		fmt.Println("[*] 1Seconds done.Waiting...")
		w.Wait("2Seconds")
		fmt.Println("[*] 1Seconds OK")
	}()

	go func() {
		fmt.Println("[-] 2Seconds Running")
		time.Sleep(1 * time.Second)
		fmt.Println("[*] 2Seconds done.Waiting...")
		w.Done("2Seconds")
		fmt.Println("[*] 2Seconds OK")
	}()

	w.WaitAll(1)
	w.Close()
	fmt.Println("Everything works.")
}
