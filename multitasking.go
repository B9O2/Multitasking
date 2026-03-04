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

type Result[TaskType any] interface {
	IsRetry() bool
	RawTask() Task[TaskType]
}

type RetryResult[TaskType any] struct {
	rawTask Task[TaskType]
	tasks   []TaskType
}

func (rr RetryResult[TaskType]) RawTask() Task[TaskType] {
	return rr.rawTask
}

func (rr RetryResult[TaskType]) Tasks() []TaskType {
	return rr.tasks
}

func (rr RetryResult[TaskType]) IsRetry() bool {
	return true
}

type NormalResult[TaskType any] struct {
	rawTask Task[TaskType]
	data    TaskType
}

func (nr NormalResult[TaskType]) RawTask() Task[TaskType] {
	return nr.rawTask
}

func (nr NormalResult[TaskType]) Data() TaskType {
	return nr.data
}

func (nr NormalResult[TaskType]) IsRetry() bool {
	return false
}

type Task[TaskType any] struct {
	isRetry bool
	data    TaskType
}

type Multitasking[TaskType any] struct {
	//property
	name    string
	debug   bool
	inherit *Multitasking[TaskType]

	//callback
	taskCallback      func(DistributeController[TaskType])
	execCallback      func(ExecuteController[TaskType], zerolog.Logger, TaskType) TaskType
	resultMiddlewares []Middleware[TaskType]
	errCallback       func(Controller[TaskType], error)
	loggerInit        func(uint64, zerolog.Logger) zerolog.Logger

	//control
	terminating bool
	pauseChan   chan struct{}
	taskQueue   chan TaskType
	retryQueue  *chanx.UnboundedChan[TaskType]
	ctx         context.Context
	cancel      context.CancelFunc

	//controller
	dc DistributeController[TaskType]
	ec ExecuteController[TaskType]

	//status
	maxRetryQueue, totalRetry, totalResult, totalTask uint64
	threadsDetail                                     *status.ThreadsDetail
	loggers                                           []zerolog.Logger

	//events []Event

	//extra
	shield *Shield.Shield
}

func (m *Multitasking[TaskType]) init(ctx context.Context, threads uint64) {
	if ctx == nil {
		ctx = context.Background()
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.shield = Shield.NewShield()
	m.terminating = false

	m.taskQueue = make(chan TaskType)
	m.retryQueue = chanx.NewUnboundedChan[TaskType](context.Background(), 1)
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

func (m *Multitasking[TaskType]) addTask(taskInfo TaskType) {
	m.taskQueue <- taskInfo
	m.totalTask += 1
	//m.Log(-2, "Join task successfully")
}

func (m *Multitasking[TaskType]) retry(taskInfo TaskType) {
	m.retryQueue.In <- taskInfo
	m.totalRetry += 1
	bl := m.retryQueue.BufLen()
	if bl > int(m.maxRetryQueue) {
		m.maxRetryQueue = uint64(bl)
	}
	//m.Log(-2, "retry task successfully")
}

func (m *Multitasking[TaskType]) SetResultMiddlewares(
	rms ...Middleware[TaskType],
) {
	m.resultMiddlewares = append(m.resultMiddlewares, rms...)
}

// func (m *Multitasking[TaskType]) Log(level int, text string) {
// 	if m.debug {
// 		fmt.Println(text)
// 	}

// 	m.events = append(m.events, Event{
// 		Level: level,
// 		Text:  text,
// 		Time:  time.Now(),
// 	})
// }

// func (m *Multitasking[TaskType]) Events(level int) []Event {
// 	var events []Event
// 	for _, event := range m.events {
// 		if event.Level <= level {
// 			events = append(events, event)
// 		}
// 	}
// 	return events
// }

func (m *Multitasking[TaskType]) Name() string {
	return m.name
}

func (m *Multitasking[TaskType]) String() string {
	//return fmt.Sprintf("\n%s(%s)\n\\_Total Tasks: %d/%d(Retry: %d MaxRetryWaiting: %d)\n", m.name, m.status, m.totalResult, m.totalTask, m.totalRetry, m.maxRetryBlock)
	return m.name
}

func (m *Multitasking[TaskType]) TotalTask() uint64 {
	return m.totalTask
}

func (m *Multitasking[TaskType]) TotalRetry() uint64 {
	return m.totalRetry
}

func (m *Multitasking[TaskType]) TotalResult() uint64 {
	return m.totalResult
}

func (m *Multitasking[TaskType]) MaxRetryQueue() uint64 {
	return m.maxRetryQueue
}

func (m *Multitasking[TaskType]) ThreadsDetail() *status.ThreadsDetail {
	return m.threadsDetail
}

func (m *Multitasking[TaskType]) Terminate() {
	m.terminating = true
	m.cancel()
}

func (m *Multitasking[TaskType]) SetErrorCallback(
	callback func(Controller[TaskType], error),
) {
	m.errCallback = callback
}

func (m *Multitasking[TaskType]) SetController(ctrl Controller[TaskType]) {
	ctrl.Init(m)
	switch c := ctrl.(type) {
	case DistributeController[TaskType]:
		m.dc = c
	case ExecuteController[TaskType]:
		m.ec = c
	default:
		//m.Log(-2, fmt.Sprintf("unknown controller type '%s'", reflect.TypeOf(c).String()))
	}
}

func (m *Multitasking[TaskType]) SetLogger(
	f func(uint64, zerolog.Logger) zerolog.Logger,
) {
	if f == nil {
		m.loggerInit = func(tid uint64, l zerolog.Logger) zerolog.Logger {
			return l.Output(os.Stdout).
				With().
				Int("thread_id", int(tid)).
				Timestamp().
				Logger()
		}
	} else {
		m.loggerInit = f
	}
}

func (m *Multitasking[TaskType]) Register(
	taskFunc func(DistributeController[TaskType]),
	execFunc func(ExecuteController[TaskType], zerolog.Logger, TaskType) TaskType,
) {
	m.taskCallback = taskFunc
	m.execCallback = execFunc
}

func (m *Multitasking[TaskType]) protect(f func()) error {
	return m.shield.Protect(f)
}

func (m *Multitasking[TaskType]) pause() {
	<-m.pauseChan
	//m.Log(1, fmt.Sprint("Pause Channel:", ok))
	m.pauseChan = make(chan struct{})
}

func (m *Multitasking[TaskType]) resume() {
	defer func() {
		fmt.Println(recover())
	}()
	close(m.pauseChan)
}

func (m *Multitasking[TaskType]) Run(
	ctx context.Context,
	threads uint64,
) (results []TaskType, err error) {
	if threads <= 0 {
		return nil, errors.New("threads should be grant than 0")
	}

	m.init(ctx, threads)

	//control
	sgw := NewWaiter() //Scheduling Gate Waiter
	bufferQueue := make(chan Task[TaskType])
	resultQueue := make(chan Result[TaskType])
	totalTaskWg := &sync.WaitGroup{}
	totalExecWg := &sync.WaitGroup{}
	resultWg := &sync.WaitGroup{}

	terminateErrorIgnore := []string{
		"multitasking terminated",
		"send on closed channel",
	}

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
				bufferQueue <- Task[TaskType]{true, task}
			case task, ok := <-m.taskQueue:
				if ok {
					totalTaskWg.Add(1)
					bufferQueue <- Task[TaskType]{false, task}
				} else if goon {
					sgw.Done("SchedulingGate") //确保totalTaskWg的Add在Wait之前完成
					goon = false
				}
			}
			<-m.pauseChan
		}

		for task := range m.retryQueue.Out {
			bufferQueue <- Task[TaskType]{true, task}
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
				var res Result[TaskType]
				var ret TaskType
				Try(func() {

					if !m.terminating {
						ret = m.execCallback(m.ec, logger, task.data)
					} else {
						res = nil //终止情况下重试任务结果直接置空(nil)
					}

				}, func(msg string) {
					m.errCallback(m.ec, errors.New(msg))
				}, terminateErrorIgnore)

				//根据返回类型的不同处理
				if rt, ok := any(ret).(RetryResult[TaskType]); ok {
					rt.rawTask = task
					if len(rt.tasks) <= 0 {
						rt.tasks = []TaskType{task.data}
					}
					res = rt
				} else {
					res = NormalResult[TaskType]{
						rawTask: task,
						data:    ret,
					}
				}
				resultQueue <- res
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
			case RetryResult[TaskType]:
				for _, rTask := range rt.Tasks() {
					m.retry(rTask)
				}
			case NormalResult[TaskType]:
				m.totalResult += 1
				totalTaskWg.Done()
				r := rt.Data()
				for _, rm := range m.resultMiddlewares {
					Try(func() {
						r = rm(m.ec, r)
					}, func(s string) {
						m.errCallback(m.ec, errors.New(s))
					}, terminateErrorIgnore)
				}
				results = append(results, r)
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

	return results, nil
}

func newMultitasking[TaskType any](
	name string,
	inherit *Multitasking[TaskType],
	debug bool,
) *Multitasking[TaskType] {
	mt := &Multitasking[TaskType]{
		name:      name,
		debug:     debug,
		inherit:   inherit,
		pauseChan: nil,
	}

	dc := NewBaseDistributeController[TaskType]()
	ec := NewBaseExecuteController[TaskType]()

	mt.SetController(dc)
	mt.SetController(ec)
	mt.SetLogger(nil)
	mt.SetErrorCallback(func(c Controller[TaskType], err error) {
		//mt.Log(0, reflect.TypeOf(c).Name()+":"+err.Error())
	})
	return mt
}

// NewMultitasking 实例化一个多线程管理实例。如果需要嵌套，此实例应处于最上层。
func NewMultitasking[TaskType any](
	name string,
	inherit *Multitasking[TaskType],
) *Multitasking[TaskType] {
	lrm := newMultitasking(name, inherit, false)
	return lrm
}
