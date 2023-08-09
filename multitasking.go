package Multitasking

import (
	"context"
	"errors"
	"fmt"
	"github.com/B9O2/NStruct/Shield"
	"strings"
	"sync"
	"time"
)

type Middleware interface {
	Run(*Multitasking, interface{}) interface{}
}

type BaseMiddleware struct {
	f   func(interface{}) (interface{}, error)
	err error
}

func (bm BaseMiddleware) Run(i interface{}) interface{} {
	var res interface{}
	res, bm.err = bm.f(i)
	return res
}

func (bm BaseMiddleware) Error() error {
	return bm.err
}

// NewBaseMiddleware 实例化一个基础的Middleware
func NewBaseMiddleware(f func(interface{}) (interface{}, error)) *BaseMiddleware {
	return &BaseMiddleware{
		f: f,
	}
}

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

type Multitasking struct {
	name                   string
	threads                int
	taskCallback           func() bool //是否已提前终止
	execCallback           func(interface{}, context.Context) interface{}
	resultMiddlewares      []Middleware
	errCallback            func(*Multitasking, error) interface{}
	taskQueue              chan interface{}
	resultChan             chan interface{}
	shield                 *Shield.Shield
	execwg                 sync.WaitGroup
	ctx                    context.Context
	cancel                 context.CancelFunc
	status                 int //0 未注册 1 已准备好 2 已执行
	totalTask, totalResult int
	runError               error
	events                 []Event
}

// AddTask 增加任务。此方法应当在任务分发函数中调用。
func (m *Multitasking) AddTask(taskInfo interface{}) {
	m.taskQueue <- taskInfo
	m.totalTask += 1

	//utils.DebugEcho("Join task successfully")
}

func (m *Multitasking) SetResultMiddlewares(rms ...Middleware) {
	for _, rm := range rms {
		m.resultMiddlewares = append(m.resultMiddlewares, rm)
	}
}

// Terminate 终止运行。此方法可以在任何位置调用。
func (m *Multitasking) Terminate() {
	if m.cancel != nil {
		m.cancel()
	}
}

func (m *Multitasking) Log(level int, text string) {
	m.events = append(m.events, Event{
		Level: level,
		Text:  text,
		Time:  time.Now(),
	})
}

func (m *Multitasking) Events(level int) []Event {
	var events []Event
	for _, event := range m.events {
		if event.Level <= level {
			events = append(events, event)
		}
	}
	return events
}

func (m *Multitasking) Name() string {
	return m.name
}

func (m *Multitasking) String() string {
	status := ""
	switch m.status {
	case 0:
		status = "Not Register"
	case 1:
		status = "Ready"
	case 2:
		status = "Done"
	default:
		status = "Unknown Status"
	}

	errText := "<nil>"
	if m.runError != nil {
		errText = m.runError.Error()
	}
	return fmt.Sprintf("\n%s(%s)\n\\_Threads: %d\n\\_Total Tasks: %d/%d\n\\_Error: %s", m.name, status, m.threads, m.totalResult, m.totalTask, errText)
}

func (m *Multitasking) SetErrorCallback(ec func(*Multitasking, error) interface{}) {
	m.errCallback = ec
}

func (m *Multitasking) Register(taskFunc func(*Multitasking), execFunc func(*Multitasking, interface{}, context.Context) interface{}) {
	m.taskCallback = func() (preStop bool) {
		preStop = false
		defer func() {
			if r := recover(); r != nil {
				err := r.(error)
				if strings.Contains(err.Error(), "send on closed channel") {
					m.runError = errors.New("stopped")
					preStop = true
				} else {
					m.runError = err
				}
			}
		}()
		taskFunc(m)
		return
	}

	m.execCallback = func(i interface{}, ctx context.Context) (ret interface{}) {
		defer func() {
			if r := recover(); r != nil {
				if m.errCallback != nil {
					ret = m.errCallback(m, r.(error))
				}
			}
		}()
		ret = execFunc(m, i, ctx)
		return
	}
	if m.status == 0 {
		m.status = 1
	}
}

func (m *Multitasking) Protect(f func() error) error {
	return m.shield.Protect(f)
}

func (m *Multitasking) FetchError() error {
	return m.runError
}

func (m *Multitasking) Run() ([]interface{}, error) {
	preStop := true

	if m.status == 2 {
		return nil, errors.New("Multitasking '" + m.name + "' is not be allowed to run again")
	} else {
		m.status = 2
	}
	var result []interface{}
	if m.taskCallback == nil || m.execCallback == nil {
		return nil, errors.New("Multitasking '" + m.name + "' must be registered")
	}

	go func() {
		preStop = m.taskCallback()
		if !preStop {
			close(m.taskQueue)
		}
	}() //启动任务写入进程

	for i := 0; i < m.threads; { //启动任务执行进程
		m.execwg.Add(1)
		go func(tid int) {
			goon := true
			defer m.execwg.Done()
			for task := range m.taskQueue {
				select {
				case <-m.ctx.Done():
					goon = false
				default:
				}
				if !goon {
					break
				}
				var ret interface{}
				ret = m.execCallback(task, m.ctx)
				m.resultChan <- ret
			}
		}(i)
		i += 1
	}
	stop := make(chan struct{})
	go func() { //启动执行结果读取进程
		for r := range m.resultChan {
			m.totalResult += 1
			for _, rm := range m.resultMiddlewares {
				r = rm.Run(m, r)
			}
			result = append(result, r)
		}
		close(stop)
	}()

	m.execwg.Wait()
	if preStop {
		close(m.taskQueue)
	}
	close(m.resultChan)
	<-stop
	return result, nil
}

func newMultitasking(name string, threads int) *Multitasking {
	return &Multitasking{
		name:       name,
		threads:    threads,
		taskQueue:  make(chan interface{}, threads),
		resultChan: make(chan interface{}, threads),
		execwg:     sync.WaitGroup{},
		shield:     Shield.NewShield(),
	}
}

// NewMultitasking 实例化一个多线程管理实例。如果需要嵌套，此实例应处于最上层。
func NewMultitasking(name string, threads int) *Multitasking {
	lrm := newMultitasking(name, threads)
	lrm.ctx, lrm.cancel = context.WithCancel(context.Background())
	return lrm
}

// NewMultitaskingWithContext 带有上下文(Context)的实例化方法。
func NewMultitaskingWithContext(name string, threads int, ctx context.Context) *Multitasking {
	lrm := newMultitasking(name, threads)
	lrm.ctx, lrm.cancel = context.WithCancel(ctx)
	return lrm
}

// NewMultitaskingWithDeadline 带有自动停止时间的实例化方法。
func NewMultitaskingWithDeadline(name string, threads int, t time.Time) *Multitasking {
	lrm := newMultitasking(name, threads)
	lrm.ctx, lrm.cancel = context.WithDeadline(context.Background(), t)
	return lrm
}

// NewMultitaskingWithTimeout 带有超时自动停止的实例化方法。
func NewMultitaskingWithTimeout(name string, threads int, d time.Duration) *Multitasking {
	lrm := newMultitasking(name, threads)
	lrm.ctx, lrm.cancel = context.WithTimeout(context.Background(), d)
	return lrm
}
