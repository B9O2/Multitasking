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

type Result interface {
	RawTask() Task
}

type RetryResult struct {
	rawTask Task
	tasks   []any
}

func (rr RetryResult) RawTask() Task {
	return rr.rawTask
}

func (rr RetryResult) Tasks() []any {
	return rr.tasks
}

type NormalResult struct {
	rawTask Task
	data    any
}

func (nr NormalResult) RawTask() Task {
	return nr.rawTask
}

func (nr NormalResult) Data() any {
	return nr.data
}

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
	name  string
	debug bool

	//callback
	taskCallback      func()
	execCallback      func(interface{}) interface{}
	resultMiddlewares []Middleware
	errCallback       func(Controller, error) interface{}

	//controller
	dc DistributeController
	ec ExecuteController

	//channel
	taskQueue   chan interface{}
	retryQueue  *chanx.UnboundedChan[any]
	bufferQueue chan Task
	resultChan  chan interface{}

	//control
	retryIndex int
	retrywg    sync.WaitGroup
	taskwg     sync.WaitGroup //raw task (not retrying-task)
	execwg     sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc

	//statics
	status                                            MTStatus
	totalRetry, totalTask, totalResult, maxRetryBlock uint
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

// Terminate 终止运行。此方法可以在任何位置调用。
func (m *Multitasking) Terminate() {
	m.status = Terminating
	if m.cancel != nil {
		m.cancel()
	}
}

func (m *Multitasking) Log(level int, text string) {
	if m.debug {
		fmt.Println(text)
	}

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
	return fmt.Sprintf("\n%s(%s)\n\\_Total Tasks: %d/%d(Retry: %d MaxRetryWaiting: %d)\n", m.name, m.status, m.totalResult, m.totalTask, m.totalRetry, m.maxRetryBlock)
}

func (m *Multitasking) SetErrorCallback(callback func(Controller, error) any) {
	m.errCallback = callback
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
				m.errCallback(m.dc, r.(error))
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

func (m *Multitasking) protect(f func()) error {
	return m.shield.Protect(f)
}

func (m *Multitasking) Close() {
	m.shield.Close()
}

func (m *Multitasking) Run(threads int) ([]interface{}, error) {
	if threads <= 0 {
		return nil, errors.New("threads should be grant than 0")
	}
	preStop := true
	closeGatePlease := make(chan byte)
	closeGateOK := make(chan byte)

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
		close(closeGatePlease)
		<-closeGateOK
		m.taskwg.Wait() //确保全部初始任务执行完毕
		m.retrywg.Wait()
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
			case <-closeGatePlease:
				loop = false
				close(closeGateOK)
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
				m.Log(-1, fmt.Sprintf("[>]DC: task.data: %v", task))
				select {
				case <-m.ctx.Done():
					goon = false
				default:
				}
				if !goon {
					break
				}

				ret := m.execCallback(task.data)

				//根据返回类型的不同处理
				if rt, ok := ret.(RetryResult); ok {
					rt.rawTask = task
					if len(rt.tasks) <= 0 {
						rt.tasks = []any{task.data}
					}
					ret = rt
				} else {
					ret = NormalResult{
						rawTask: task,
						data:    ret,
					}
				}

				m.resultChan <- ret
			}
		}(i)
		m.Log(-1, fmt.Sprintf("[+] task execute started (%d)", i))
		i += 1
	}

	stop := make(chan struct{})
	go func() { //启动执行结果收集线程
		for ret := range m.resultChan {
			switch ret.(type) {
			case RetryResult:
				rt := ret.(RetryResult)
				for _, rTask := range rt.Tasks() {
					m.retry(rTask)
				}
				if !rt.RawTask().isRetry {
					//非递归重试任务
					m.retrywg.Add(len(rt.tasks))
					//m.retryIndex += len(rt.tasks)
				}
			case NormalResult:
				rt := ret.(NormalResult)
				if rt.RawTask().isRetry {
					m.retrywg.Done()
					//m.retryIndex -= 1
					//fmt.Println(m.retryIndex)
				}
				r := rt.Data()
				for _, rm := range m.resultMiddlewares {
					func() {
						defer func() {
							if rec := recover(); rec != nil {
								r = m.errCallback(m.ec, rec.(error))
							}
						}()
						r = rm.Run(m.ec, r)
					}()
				}
				result = append(result, r)
				m.totalResult += 1
			}
			if !ret.(Result).RawTask().isRetry {
				m.taskwg.Done()
			}
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

func newMultitasking(name string, inherit *Multitasking, debug bool) *Multitasking {
	mt := &Multitasking{
		name:        name,
		debug:       debug,
		taskQueue:   make(chan interface{}),
		retryQueue:  chanx.NewUnboundedChan[any](context.Background(), 1),
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
	lrm := newMultitasking(name, inherit, false)
	lrm.ctx, lrm.cancel = context.WithCancel(context.Background())
	return lrm
}

// NewMultitaskingWithContext 带有上下文(Context)的实例化方法。
func NewMultitaskingWithContext(name string, inherit *Multitasking, ctx context.Context) *Multitasking {
	lrm := newMultitasking(name, inherit, false)
	lrm.ctx, lrm.cancel = context.WithCancel(ctx)
	return lrm
}

// NewMultitaskingWithDeadline 带有自动停止时间的实例化方法。
func NewMultitaskingWithDeadline(name string, inherit *Multitasking, t time.Time) *Multitasking {
	lrm := newMultitasking(name, inherit, false)
	lrm.ctx, lrm.cancel = context.WithDeadline(context.Background(), t)
	return lrm
}

// NewMultitaskingWithTimeout 带有超时自动停止的实例化方法。
func NewMultitaskingWithTimeout(name string, inherit *Multitasking, d time.Duration) *Multitasking {
	lrm := newMultitasking(name, inherit, false)
	lrm.ctx, lrm.cancel = context.WithTimeout(context.Background(), d)
	return lrm
}
