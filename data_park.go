package Multitasking

import (
	"github.com/B9O2/NStruct/Shield"
)

type DataPark struct {
	s     *Shield.Shield
	queue map[string][]chan any
	data  map[string]any
}

func (dp *DataPark) Put(key string, value any) {
	dp.s.Protect(func() {
		dp.data[key] = value
		for _, ch := range dp.queue[key] {
			ch <- value
		}
	})
}

func (dp *DataPark) Require(key string) any {
	var res any
	var ch chan any
	dp.s.Protect(func() {
		if v, ok := dp.data[key]; ok {
			res = v
		} else {
			ch = make(chan any)
			dp.queue[key] = append(dp.queue[key], ch)
		}
	})
	if ch != nil {
		res = <-ch
	}
	return res
}

func NewDataPark() *DataPark {
	dp := &DataPark{
		s:     Shield.NewShield(),
		queue: map[string][]chan any{},
		data:  map[string]any{},
	}
	return dp
}
