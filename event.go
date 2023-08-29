package Multitasking

import (
	"fmt"
	"time"
)

type Event struct {
	Level int
	Text  string
	Time  time.Time
}

func (e Event) String() string {
	return fmt.Sprintf("%s <%d> %s",
		e.Time.AppendFormat(nil, "01/02 15:04:05 2006"),
		e.Level,
		e.Text)
}
