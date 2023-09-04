package Multitasking

import (
	"context"
	"errors"
	"fmt"
	"github.com/B9O2/NStruct/Shield"
	"github.com/smallnest/chanx"
	"reflect"
	"strings"
	"sync"
	"time"
)

type Task struct {
	isRetry bool
	data    any
}

type MTStatus int

func (ms MTStatus) String() string {
	status := ""
	switch ms {
	case 0:
		status = "Not Register"
	case 1:
		status = "Ready"
	case 2:
		status = "Running"
	case 3:
		status = "Done"
	case 4:
		status = "Terminating"
	case 5:
		status = "Terminated"
	default:
		status = "Unknown Status"
	}
	return status
}

const (
	NotRegister MTStatus = iota
	Ready
	Running
	Done
	Terminating
	Terminated
)

type Multitasking struct {
	//property
	name string

	//callback
	taskCallback      func()
	execCallback      func(interface{}) interface{}
	resultMiddlewares []Middleware
	errCallback       func(ExecuteController, error) interface{}

	//controller
	dc DistributeController
	ec ExecuteController

	//channel
	taskQueue   chan interface{}
	retryQueue  *chanx.UnboundedChan[any]
	bufferQueue chan Task
	resultChan  chan interface{}

	//control
	taskwg sync.WaitGroup //raw task (not retrying-task)
	execwg sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	//statics
	status                                            MTStatus
	totalRetry, totalTask, totalResult, maxRetryBlock uint
	runError                                          error
	events                                            []Event

	//extra
	shield *Shield.Shield
}

// addTask 增加任务。此方法应当在任务分发函数中调用。(在任务执行过程中调用将导致死锁)
func (m *Multitasking) addTask(taskInfo interface{}) {
	m.taskQueue <- taskInfo
	m.totalTask += 1

	//m.Log(-2, "Join task successfully")
}

// retry 重试任务。此方法应当在任务执行过程中调用
func (m *Multitasking) retry(taskInfo interface{}) {
	m.retryQueue.In <- taskInfo
	bl := m.retryQueue.BufLen()
	if bl > int(m.maxRetryBlock) {
		m.maxRetryBlock = uint(bl)
	}
	//m.Log(-2, "retry task successfully")
}

func (m *Multitasking) SetResultMiddlewares(rms ...Middleware) {
	for _, rm := range rms {
		m.resultMiddlewares = append(m.resultMiddlewares, rm)
	}
}

// terminate 终止运行。此方法可以在任何位置调用。
func (m *Multitasking) terminate() {
	m.status = Terminating
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
	errText := "<nil>"
	if m.runError != nil {
		errText = m.runError.Error()
	}
	return fmt.Sprintf("\n%s(%s)\n\\_Total Tasks: %d/%d(Retry: %d MaxRetryWaiting: %d)\n\\_Error: %s", m.name, m.status, m.totalResult, m.totalTask, m.totalRetry, m.maxRetryBlock, errText)
}

func (m *Multitasking) SetErrorCallback(ec func(ExecuteController, error) any) {
	m.errCallback = ec
}

func (m *Multitasking) SetController(c Controller) {
	switch c.(type) {
	case DistributeController:
		m.dc = c.(DistributeController)
	case ExecuteController:
		m.ec = c.(ExecuteController)
	default:
		m.Log(-2, fmt.Sprintf("unknown controller type '%s'", reflect.TypeOf(c).String()))
	}
}

func (m *Multitasking) Register(taskFunc func(DistributeController), execFunc func(ExecuteController, any) any) {
	m.taskCallback = func() {
		defer func() {
			if r := recover(); r != nil {
				m.runError = errors.New(fmt.Sprintf("%v", r))
			}
		}()
		taskFunc(m.dc)
		return
	}

	m.execCallback = func(i interface{}) (ret interface{}) {
		defer func() {
			if r := recover(); r != nil {
				if m.errCallback != nil {
					ret = m.errCallback(m.ec, r.(error))
				}
			}
		}()
		ret = execFunc(m.ec, i)
		return
	}
	if m.status == NotRegister {
		m.status = Ready
	}
}

func (m *Multitasking) protect(f func() error) error {
	return m.shield.Protect(f)
}

func (m *Multitasking) FetchError() error {
	return m.runError
}

func (m *Multitasking) Run(threads int) ([]interface{}, error) {
	if threads <= 0 {
		return nil, errors.New("threads should be grant than 0")
	}
	preStop := true
	closeGate := make(chan byte)

	if m.status == Done || m.status == Terminated {
		return nil, errors.New("Multitasking '" + m.name + "' is not be allowed to run again")
	} else {
		m.status = Running
	}
	var result []interface{}
	if m.taskCallback == nil || m.execCallback == nil {
		return nil, errors.New("Multitasking '" + m.name + "' must be registered")
	}

	go func() {
		m.taskCallback()
		m.Log(-2, "[-] task distribute closed")
		close(closeGate)
		m.taskwg.Wait()
		close(m.retryQueue.In)
	}() //启动任务分发线程
	m.Log(-2, "[+] task distribute started")

	go func() {
		loop := true
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Sprintf("%v", r)
				if strings.Contains(err, "send on closed channel") {
					m.Log(-2, "[-] task gate closed (Terminate)")
				} else {
					m.Log(-2, fmt.Sprintf("[-] task gate closed (err:%s)", err))
					m.runError = errors.New(err)
				}
			} else {
				m.Log(-2, "[-] task gate closed")
			}
			//Terminated
		}()

		for loop {
			select {
			case task := <-m.retryQueue.Out:
				m.bufferQueue <- Task{true, task}
				m.totalRetry += 1
				//m.totalTask += 1
			case task := <-m.taskQueue:
				m.taskwg.Add(1)
				m.bufferQueue <- Task{false, task}

			case <-closeGate:
				loop = false
			}
		}
		for task := range m.retryQueue.Out {
			m.bufferQueue <- Task{true, task}
			m.totalRetry += 1
		}
		preStop = false
		close(m.bufferQueue)
	}() //Gate
	m.Log(-2, "[+] task gate started")

	for i := 0; i < threads; { //启动任务执行线程
		m.execwg.Add(1)
		go func(tid int) {
			goon := true
			defer func() {
				m.execwg.Done()
				m.Log(-1, fmt.Sprintf("[-] task execute closed (%d)", tid))
			}()
			for task := range m.bufferQueue {
				select {
				case <-m.ctx.Done():
					goon = false
				default:
				}
				if !goon {
					break
				}
				var ret interface{}
				ret = m.execCallback(task.data)

				if !task.isRetry {
					m.taskwg.Done()
				}

				m.resultChan <- ret
			}
		}(i)
		m.Log(-1, fmt.Sprintf("[+] task execute started (%d)", i))
		i += 1
	}

	stop := make(chan struct{})
	go func() { //启动执行结果收集线程
		for r := range m.resultChan {
			for _, rm := range m.resultMiddlewares {
				r = rm.Run(m.ec, r)
			}
			result = append(result, r)
			m.totalResult += 1
		}
		close(stop)
		m.Log(-2, "[-] result collector closed")
	}()
	m.Log(-2, "[+] result collector started")

	m.execwg.Wait()
	if preStop {
		close(m.bufferQueue)
	}
	close(m.resultChan)
	<-stop
	if m.status == Terminating {
		m.status = Terminated
	} else {
		m.status = Done
	}
	return result, nil
}

func newMultitasking(name string, inherit *Multitasking) *Multitasking {
	mt := &Multitasking{
		name:        name,
		taskQueue:   make(chan interface{}),
		retryQueue:  chanx.NewUnboundedChan[any](1),
		bufferQueue: make(chan Task),
		resultChan:  make(chan interface{}),
		execwg:      sync.WaitGroup{},
		shield:      Shield.NewShield(),
	}
	dc := &BaseDistributeController{
		NewBaseController(mt, inherit),
	}
	ec := &BaseExecuteController{
		NewBaseController(mt, inherit),
	}
	mt.SetController(dc)
	mt.SetController(ec)
	return mt
}

// NewMultitasking 实例化一个多线程管理实例。如果需要嵌套，此实例应处于最上层。
func NewMultitasking(name string, inherit *Multitasking) *Multitasking {
	lrm := newMultitasking(name, inherit)
	lrm.ctx, lrm.cancel = context.WithCancel(context.Background())
	return lrm
}

// NewMultitaskingWithContext 带有上下文(Context)的实例化方法。
func NewMultitaskingWithContext(name string, inherit *Multitasking, ctx context.Context) *Multitasking {
	lrm := newMultitasking(name, inherit)
	lrm.ctx, lrm.cancel = context.WithCancel(ctx)
	return lrm
}

// NewMultitaskingWithDeadline 带有自动停止时间的实例化方法。
func NewMultitaskingWithDeadline(name string, inherit *Multitasking, t time.Time) *Multitasking {
	lrm := newMultitasking(name, inherit)
	lrm.ctx, lrm.cancel = context.WithDeadline(context.Background(), t)
	return lrm
}

// NewMultitaskingWithTimeout 带有超时自动停止的实例化方法。
func NewMultitaskingWithTimeout(name string, inherit *Multitasking, d time.Duration) *Multitasking {
	lrm := newMultitasking(name, inherit)
	lrm.ctx, lrm.cancel = context.WithTimeout(context.Background(), d)
	return lrm
}
