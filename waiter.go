package Multitasking

import (
	"github.com/B9O2/NStruct/Shield"
	"sync"
)

type process struct {
	conditions map[string]struct{}
	wg         *sync.WaitGroup
}

type Waiter struct {
	s          *Shield.Shield
	waiters    []process
	waiterDone map[string]struct{}
	processWg  *sync.WaitGroup
}

func (w *Waiter) Done(name string) {
	w.s.Protect(func() {
		for _, p := range w.waiters {
			if _, ok := p.conditions[name]; ok {
				p.wg.Done()
			}
		}
		w.waiterDone[name] = struct{}{}
	})
}

func (w *Waiter) Wait(conditions ...string) {
	defer func() {
		//fmt.Println(conditions, " END")
	}()

	proc := process{
		wg: &sync.WaitGroup{},
	}
	proc.wg.Add(len(conditions))

	conditionsMap := map[string]struct{}{}
	w.s.Protect(func() {
		for _, condition := range conditions {
			conditionsMap[condition] = struct{}{}
			if _, done := w.waiterDone[condition]; done {
				proc.wg.Done()
			}
		}
		proc.conditions = conditionsMap

		w.waiters = append(w.waiters, proc)
	})

	proc.wg.Wait()
	w.processWg.Done()
}

func (w *Waiter) WaitAll(n uint) {
	w.processWg.Add(int(n))
	w.processWg.Wait()
}

func (w *Waiter) Close() {
	w.s.Close()
}

func NewWaiter() *Waiter {
	return &Waiter{
		s:          Shield.NewShield(),
		waiters:    make([]process, 0),
		processWg:  &sync.WaitGroup{},
		waiterDone: map[string]struct{}{},
	}
}
