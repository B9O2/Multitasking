package Multitasking

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/B9O2/Multitasking/status"
	"github.com/B9O2/NStruct/Shield"
	"github.com/rs/zerolog"
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
	execCallback      func(ExecuteController, zerolog.Logger, interface{}) interface{}
	resultMiddlewares []Middleware
	errCallback       func(Controller, error)
	loggerInit        func(uint64, zerolog.Logger) zerolog.Logger

	//control
	terminating bool
	pauseChan   chan struct{}
	taskQueue   chan interface{}
	retryQueue  *chanx.UnboundedChan[any]
	ctx         context.Context
	cancel      context.CancelFunc

	//controller
	dc DistributeController
	ec ExecuteController

	//status
	maxRetryQueue, totalRetry, totalResult, totalTask uint64
	threadsDetail                                     *status.ThreadsDetail
	loggers                                           []zerolog.Logger

	//events []Event

	//extra
	shield *Shield.Shield
}

func (m *Multitasking) init(ctx context.Context, threads uint64) {
	if ctx == nil {
		ctx = context.Background()
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.shield = Shield.NewShield()
	m.terminating = false

	m.taskQueue = make(chan interface{})
	m.retryQueue = chanx.NewUnboundedChan[any](context.Background(), 1)
	m.pauseChan = make(chan struct{})
	close(m.pauseChan)

	//status
	m.totalRetry = 0
	m.totalTask = 0
	m.totalResult = 0
	m.maxRetryQueue = 0
	m.threadsDetail = status.NewThreadsDetail(threads)
	m.loggers = make([]zerolog.Logger, threads)
}

func (m *Multitasking) addTask(taskInfo interface{}) {
	m.taskQueue <- taskInfo
	m.totalTask += 1
	//m.Log(-2, "Join task successfully")
}

func (m *Multitasking) retry(taskInfo interface{}) {
	m.retryQueue.In <- taskInfo
	m.totalRetry += 1
	bl := m.retryQueue.BufLen()
	if bl > int(m.maxRetryQueue) {
		m.maxRetryQueue = uint64(bl)
	}
	//m.Log(-2, "retry task successfully")
}

func (m *Multitasking) SetResultMiddlewares(rms ...Middleware) {
	m.resultMiddlewares = append(m.resultMiddlewares, rms...)
}

// func (m *Multitasking) Log(level int, text string) {
// 	if m.debug {
// 		fmt.Println(text)
// 	}

// 	m.events = append(m.events, Event{
// 		Level: level,
// 		Text:  text,
// 		Time:  time.Now(),
// 	})
// }

// func (m *Multitasking) Events(level int) []Event {
// 	var events []Event
// 	for _, event := range m.events {
// 		if event.Level <= level {
// 			events = append(events, event)
// 		}
// 	}
// 	return events
// }

func (m *Multitasking) Name() string {
	return m.name
}

func (m *Multitasking) String() string {
	//return fmt.Sprintf("\n%s(%s)\n\\_Total Tasks: %d/%d(Retry: %d MaxRetryWaiting: %d)\n", m.name, m.status, m.totalResult, m.totalTask, m.totalRetry, m.maxRetryBlock)
	return m.name
}

func (m *Multitasking) TotalTask() uint64 {
	return m.totalTask
}

func (m *Multitasking) TotalRetry() uint64 {
	return m.totalRetry
}

func (m *Multitasking) TotalResult() uint64 {
	return m.totalResult
}

func (m *Multitasking) MaxRetryQueue() uint64 {
	return m.maxRetryQueue
}

func (m *Multitasking) ThreadsDetail() *status.ThreadsDetail {
	return m.threadsDetail
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
		//m.Log(-2, fmt.Sprintf("unknown controller type '%s'", reflect.TypeOf(c).String()))
	}
}

func (m *Multitasking) SetLogger(f func(uint64, zerolog.Logger) zerolog.Logger) {
	if f == nil {
		m.loggerInit = func(tid uint64, l zerolog.Logger) zerolog.Logger {
			return l.Output(os.Stdout).With().Int("thread_id", int(tid)).Timestamp().Logger()
		}
	} else {
		m.loggerInit = f
	}
}

func (m *Multitasking) Register(taskFunc func(DistributeController), execFunc func(ExecuteController, zerolog.Logger, any) any) {
	m.taskCallback = taskFunc
	m.execCallback = execFunc
}

func (m *Multitasking) protect(f func()) error {
	return m.shield.Protect(f)
}

func (m *Multitasking) pause() {
	<-m.pauseChan
	//m.Log(1, fmt.Sprint("Pause Channel:", ok))
	m.pauseChan = make(chan struct{})
}

func (m *Multitasking) resume() {
	defer func() {
		fmt.Println(recover())
	}()
	close(m.pauseChan)
}

func (m *Multitasking) Run(ctx context.Context, threads uint64) (result []interface{}, err error) {
	if threads <= 0 {
		return nil, errors.New("threads should be grant than 0")
	}

	m.init(ctx, threads)

	//control
	sgw := NewWaiter() //Scheduling Gate Waiter
	bufferQueue := make(chan Task)
	resultQueue := make(chan interface{})
	totalTaskWg := &sync.WaitGroup{}
	totalExecWg := &sync.WaitGroup{}
	resultWg := &sync.WaitGroup{}

	terminateErrorIgnore := []string{"multitasking terminated", "send on closed channel"}

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
			case task, ok := <-m.taskQueue:
				if ok {
					totalTaskWg.Add(1)
					bufferQueue <- Task{false, task}
				} else if goon {
					sgw.Done("SchedulingGate") //确保totalTaskWg的Add在Wait之前完成
					goon = false
				}
			}
			<-m.pauseChan
		}

		for task := range m.retryQueue.Out {
			bufferQueue <- Task{true, task}
		}
	}, func(msg string) {
		m.errCallback(m.dc, errors.New(msg))
	}, terminateErrorIgnore)

	//Execution
	for tid := uint64(0); tid < threads; {
		exid := tid
		totalExecWg.Add(1)
		logger := m.loggerInit(tid, zerolog.New(nil))
		m.loggers[exid] = logger
		go func() {
			defer func() {
				totalExecWg.Done()
				//m.Log(-1, fmt.Sprintf("[-] task execute closed (%d)", exid))
			}()
			for task := range bufferQueue {
				m.threadsDetail.Working(exid)
				m.threadsDetail.Add(exid, 1)
				//m.Log(-1, fmt.Sprintf("[>]DC: task.data: %v", task))
				select {
				case <-m.ec.Context().Done():
					m.ec.Terminate()
				default:
				}
				var ret any
				Try(func() {

					if !m.terminating {
						ret = m.execCallback(m.ec, logger, task.data)
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
				m.threadsDetail.Idle(exid)
			}
		}()

		//m.Log(-1, fmt.Sprintf("[+] task execute started (%d)", exid))
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
					}, terminateErrorIgnore)
				}
				if r != nil {
					result = append(result, r)
				}
			}
		}
		//m.Log(-2, "[-] result collector closed")
	}, func(msg string) {
		m.errCallback(m.ec, errors.New(msg))
	}, terminateErrorIgnore)

	sgw.WaitAll(1)
	totalTaskWg.Wait()
	//m.Log(-2, "[*]All Tasks Done")
	TryClose(m.retryQueue.In)
	//m.Log(-2, "[*]Retry Closed")
	TryClose(bufferQueue)
	//m.Log(-2, "[*]BufferQueue Closed")
	m.ec.Terminate()
	//m.Log(-2, "[*]EC Terminated")
	totalExecWg.Wait()
	//m.Log(-2, "[*]Total Task Done")
	sgw.Close()
	m.shield.Close()
	close(resultQueue)
	//m.Log(-2, "[-]Waiting ResultQueue Close")
	resultWg.Wait()
	//m.Log(-2, "[*]ResultQueue Closed")

	return result, nil
}

func newMultitasking(name string, inherit *Multitasking, debug bool) *Multitasking {
	mt := &Multitasking{
		name:      name,
		debug:     debug,
		inherit:   inherit,
		pauseChan: nil,
	}

	dc := NewBaseDistributeController()
	ec := NewBaseExecuteController()

	mt.SetController(dc)
	mt.SetController(ec)
	mt.SetLogger(nil)
	mt.SetErrorCallback(func(c Controller, err error) {
		//mt.Log(0, reflect.TypeOf(c).Name()+":"+err.Error())
	})
	return mt
}

// NewMultitasking 实例化一个多线程管理实例。如果需要嵌套，此实例应处于最上层。
func NewMultitasking(name string, inherit *Multitasking) *Multitasking {
	lrm := newMultitasking(name, inherit, false)
	return lrm
}
