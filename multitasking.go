package Multitasking

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/B9O2/NStruct/Shield"
	"github.com/smallnest/chanx"
)

type Result interface {
	IsRetry() bool
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

func (rr RetryResult) IsRetry() bool {
	return true
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

func (nr NormalResult) IsRetry() bool {
	return false
}

type Task struct {
	isRetry bool
	data    any
}

type Multitasking struct {
	//property
	name    string
	debug   bool
	inherit *Multitasking

	//callback
	taskCallback      func(DistributeController)
	execCallback      func(ExecuteController, interface{}) interface{}
	resultMiddlewares []Middleware
	errCallback       func(Controller, error)

	//control
	terminating bool
	taskQueue   chan interface{}
	retryQueue  *chanx.UnboundedChan[any]
	ctx         context.Context
	cancel      context.CancelFunc

	//controller
	dc DistributeController
	ec ExecuteController

	//statics
	maxRetryBlock, totalResult, totalTask uint
	events                                []Event

	//extra
	shield *Shield.Shield
}

// addTask 增加任务。此方法应当在任务分发函数中调用。
func (m *Multitasking) addTask(taskInfo interface{}) {
	m.taskQueue <- taskInfo

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
	m.resultMiddlewares = append(m.resultMiddlewares, rms...)
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
	//return fmt.Sprintf("\n%s(%s)\n\\_Total Tasks: %d/%d(Retry: %d MaxRetryWaiting: %d)\n", m.name, m.status, m.totalResult, m.totalTask, m.totalRetry, m.maxRetryBlock)
	return m.name
}

func (m *Multitasking) Terminate() {
	m.terminating = true
	m.cancel()
}

func (m *Multitasking) SetErrorCallback(callback func(Controller, error)) {
	m.errCallback = callback
}

func (m *Multitasking) SetController(ctrl Controller) {
	ctrl.Init(m)
	switch c := ctrl.(type) {
	case DistributeController:
		m.dc = c
	case ExecuteController:
		m.ec = c
	default:

		m.Log(-2, fmt.Sprintf("unknown controller type '%s'", reflect.TypeOf(c).String()))
	}
}

func (m *Multitasking) Register(taskFunc func(DistributeController), execFunc func(ExecuteController, any) any) {
	m.taskCallback = taskFunc
	m.execCallback = execFunc
}

func (m *Multitasking) protect(f func()) error {
	return m.shield.Protect(f)
}

func (m *Multitasking) Run(ctx context.Context, threads uint) (result []interface{}, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	terminateErrorIgnore := []string{"multitasking terminated", "send on closed channel"}
	m.shield = Shield.NewShield()
	if threads <= 0 {
		return nil, errors.New("threads should be grant than 0")
	}
	m.terminating = false

	//control
	sgw := NewWaiter() //Scheduling Gate Waiter
	bufferQueue := make(chan Task)
	resultQueue := make(chan interface{})
	totalTaskWg := &sync.WaitGroup{}
	totalExecWg := &sync.WaitGroup{}
	resultWg := &sync.WaitGroup{}
	m.taskQueue = make(chan interface{})
	m.retryQueue = chanx.NewUnboundedChan[any](context.Background(), 1)

	//static
	totalRetry := 0
	m.totalTask = 0
	m.totalResult = 0

	//Distribution
	go Try(func() {
		defer func() {
			TryClose(m.taskQueue)
			sgw.Wait("SchedulingGate") //确保task里的都被加入到BufferQueue再结束
		}()
		m.taskCallback(m.dc)
	}, func(msg string) {
		m.errCallback(m.dc, errors.New(msg))
	}, terminateErrorIgnore)

	//SchedulingGate
	go Try(func() {
		goon := true
		for goon {
			select {
			case task := <-m.retryQueue.Out:
				bufferQueue <- Task{true, task}
				totalRetry += 1
			case task, ok := <-m.taskQueue:
				if ok {
					totalTaskWg.Add(1)
					bufferQueue <- Task{false, task}
				} else if goon {
					sgw.Done("SchedulingGate") //确保totalTaskWg的Add在Wait之前完成
					goon = false
				}
			}
		}

		for task := range m.retryQueue.Out {
			bufferQueue <- Task{true, task}
			totalRetry += 1
		}
	}, func(msg string) {
		m.errCallback(m.dc, errors.New(msg))
	}, terminateErrorIgnore)

	//Execution
	for tid := uint(0); tid < threads; {
		exid := tid
		totalExecWg.Add(1)
		go func() {
			defer func() {
				totalExecWg.Done()
				m.Log(-1, fmt.Sprintf("[-] task execute closed (%d)", exid))
			}()
			for task := range bufferQueue {
				m.Log(-1, fmt.Sprintf("[>]DC: task.data: %v", task))
				select {
				case <-m.ec.Context().Done():
					m.ec.Terminate()
				default:
				}
				var ret any
				Try(func() {

					if !m.terminating {
						ret = m.execCallback(m.ec, task.data)
					} else {
						ret = nil //终止情况下重试任务结果直接置空(nil)
					}

				}, func(msg string) {
					m.errCallback(m.ec, errors.New(msg))
				}, terminateErrorIgnore)

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
				resultQueue <- ret
			}
		}()

		m.Log(-1, fmt.Sprintf("[+] task execute started (%d)", exid))
		tid += 1
	}

	//Result
	resultWg.Add(1)
	go Try(func() {
		defer func() {
			resultWg.Done()
		}()
		for ret := range resultQueue {
			switch rt := ret.(type) {
			case RetryResult:
				for _, rTask := range rt.Tasks() {
					m.retry(rTask)
				}
			case NormalResult:
				m.totalResult += 1
				totalTaskWg.Done()
				r := rt.Data()
				for _, rm := range m.resultMiddlewares {
					Try(func() {
						r = rm.Run(m.ec, r)
					}, func(s string) {
						m.errCallback(m.ec, errors.New(s))
						r = nil
					}, nil)
				}
				result = append(result, r)
			}
		}
		m.Log(-2, "[-] result collector closed")
	}, func(msg string) {
		m.errCallback(m.ec, errors.New(msg))
	}, terminateErrorIgnore)

	sgw.WaitAll(1)
	totalTaskWg.Wait()
	m.Log(-2, "[*]All Tasks Done")
	TryClose(m.retryQueue.In)
	m.Log(-2, "[*]Retry Closed")
	TryClose(bufferQueue)
	m.Log(-2, "[*]BufferQueue Closed")
	m.ec.Terminate()
	m.Log(-2, "[*]EC Terminated")
	totalExecWg.Wait()
	m.Log(-2, "[*]Total Task Done")
	sgw.Close()
	m.shield.Close()
	close(resultQueue)
	m.Log(-2, "[-]Waiting ResultQueue Close")
	resultWg.Wait()
	m.Log(-2, "[*]ResultQueue Closed")

	return result, nil
}

func newMultitasking(name string, inherit *Multitasking, debug bool) *Multitasking {
	mt := &Multitasking{
		name:    name,
		debug:   debug,
		inherit: inherit,
	}

	dc := NewBaseDistributeController()
	ec := NewBaseExecuteController()

	mt.SetController(dc)
	mt.SetController(ec)
	mt.SetErrorCallback(func(c Controller, err error) {
		mt.Log(0, reflect.TypeOf(c).Name()+":"+err.Error())
	})
	return mt
}

// NewMultitasking 实例化一个多线程管理实例。如果需要嵌套，此实例应处于最上层。
func NewMultitasking(name string, inherit *Multitasking) *Multitasking {
	lrm := newMultitasking(name, inherit, false)
	return lrm
}
