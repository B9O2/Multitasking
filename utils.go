package Multitasking

import (
	"fmt"
	"strings"
)

func Try(f func(), catch func(string), ignore []string) {
	defer func() {
		if r := recover(); r != nil && catch != nil {
			msg := fmt.Sprint(r)
			for _, i := range ignore {
				if strings.Contains(msg, i) {
					return
				}
			}
			catch(msg)
		}
	}()
	f()
}

func TryClose[T any](ch chan<- T) {
	defer func() {
		recover()
	}()
	close(ch)
}
