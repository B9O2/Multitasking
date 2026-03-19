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

type Task[TaskType any] struct {
	isRetry bool
	data    TaskType
}

type Multitasking[TaskType any, ResultType any] struct {
	//property
	name    string
	debug   bool
	inherit *Multitasking[TaskType, ResultType]

	//callback
	taskCallback      func(DistributeController[TaskType, ResultType])
	execCallback      func(ExecuteController[TaskType, ResultType], TaskType) Result[TaskType, ResultType]
	resultMiddlewares []Middleware[TaskType, ResultType]
	errCallback       func(Controller[TaskType, ResultType], error)
	loggerInit        func(uint64, zerolog.Logger) zerolog.Logger

	//control
	terminating bool
	pauseChan   chan struct{}
	taskQueue   chan TaskType
	retryQueue  *chanx.UnboundedChan[TaskType]
	ctx         context.Context
	cancel      context.CancelFunc

	//controller
	dc DistributeController[TaskType, ResultType]
	ec ExecuteController[TaskType, ResultType]

	//status
	maxRetryQueue, totalRetry, totalResult, totalTask uint64
	threadsDetail                                     *status.ThreadsDetail
	loggers                                           []zerolog.Logger

	//events []Event

	//extra
	shield *Shield.Shield
}

func (m *Multitasking[TaskType, ResultType]) init(
	ctx context.Context,
	threads uint64,
) {
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

func (m *Multitasking[TaskType, ResultType]) addTask(taskInfo TaskType) {
	m.taskQueue <- taskInfo
	m.totalTask += 1
	//m.Log(-2, "Join task successfully")
}

func (m *Multitasking[TaskType, ResultType]) retry(taskInfo TaskType) {
	m.retryQueue.In <- taskInfo
	m.totalRetry += 1
	bl := m.retryQueue.BufLen()
	if bl > int(m.maxRetryQueue) {
		m.maxRetryQueue = uint64(bl)
	}
	//m.Log(-2, "retry task successfully")
}

func (m *Multitasking[TaskType, ResultType]) SetResultMiddlewares(
	rms ...Middleware[TaskType, ResultType],
) {
	m.resultMiddlewares = append(m.resultMiddlewares, rms...)
}

// func (m *Multitasking[TaskType, ResultType]) Log(level int, text string) {
// 	if m.debug {
// 		fmt.Println(text)
// 	}

// 	m.events = append(m.events, Event{
// 		Level: level,
// 		Text:  text,
// 		Time:  time.Now(),
// 	})
// }

// func (m *Multitasking[TaskType, ResultType]) Events(level int) []Event {
// 	var events []Event
// 	for _, event := range m.events {
// 		if event.Level <= level {
// 			events = append(events, event)
// 		}
// 	}
// 	return events
// }

func (m *Multitasking[TaskType, ResultType]) Name() string {
	return m.name
}

func (m *Multitasking[TaskType, ResultType]) String() string {
	//return fmt.Sprintf("\n%s(%s)\n\\_Total Tasks: %d/%d(Retry: %d MaxRetryWaiting: %d)\n", m.name, m.status, m.totalResult, m.totalTask, m.totalRetry, m.maxRetryBlock)
	return m.name
}

func (m *Multitasking[TaskType, ResultType]) TotalTask() uint64 {
	return m.totalTask
}

func (m *Multitasking[TaskType, ResultType]) TotalRetry() uint64 {
	return m.totalRetry
}

func (m *Multitasking[TaskType, ResultType]) TotalResult() uint64 {
	return m.totalResult
}

func (m *Multitasking[TaskType, ResultType]) MaxRetryQueue() uint64 {
	return m.maxRetryQueue
}

func (m *Multitasking[TaskType, ResultType]) ThreadsDetail() *status.ThreadsDetail {
	return m.threadsDetail
}

func (m *Multitasking[TaskType, ResultType]) Terminate() {
	m.terminating = true
	m.cancel()
}

func (m *Multitasking[TaskType, ResultType]) SetErrorCallback(
	callback func(Controller[TaskType, ResultType], error),
) {
	m.errCallback = callback
}

func (m *Multitasking[TaskType, ResultType]) SetController(
	ctrl Controller[TaskType, ResultType],
) {
	ctrl.Init(m)
	switch c := ctrl.(type) {
	case DistributeController[TaskType, ResultType]:
		m.dc = c
	case ExecuteController[TaskType, ResultType]:
		m.ec = c
	default:
		//m.Log(-2, fmt.Sprintf("unknown controller type '%s'", reflect.TypeOf(c).String()))
	}
}

func (m *Multitasking[TaskType, ResultType]) SetLogger(
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

func (m *Multitasking[TaskType, ResultType]) Register(
	taskFunc func(DistributeController[TaskType, ResultType]),
	execFunc func(ExecuteController[TaskType, ResultType], TaskType) Result[TaskType, ResultType],
) {
	m.taskCallback = taskFunc
	m.execCallback = execFunc
}

func (m *Multitasking[TaskType, ResultType]) protect(f func()) error {
	return m.shield.Protect(f)
}

func (m *Multitasking[TaskType, ResultType]) pause() {
	<-m.pauseChan
	//m.Log(1, fmt.Sprint("Pause Channel:", ok))
	m.pauseChan = make(chan struct{})
}

func (m *Multitasking[TaskType, ResultType]) resume() {
	defer func() {
		fmt.Println(recover())
	}()
	close(m.pauseChan)
}

func (m *Multitasking[TaskType, ResultType]) Run(
	ctx context.Context,
	threads uint64,
) (results []ResultType, err error) {
	if threads <= 0 {
		return nil, errors.New("threads should be grant than 0")
	}

	m.init(ctx, threads)

	//control
	sgw := NewWaiter() //Scheduling Gate Waiter
	bufferQueue := make(chan Task[TaskType])
	resultQueue := make(chan Result[TaskType, ResultType])
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

		threadCtx := context.WithValue(m.ctx, "thread_id", exid)
		workerEC := m.ec.WithContext(threadCtx).WithLogger(logger)

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
				case <-workerEC.Context().Done():
					workerEC.Terminate()
				default:
				}
				var res Result[TaskType, ResultType]
				Try(func() {

					if !m.terminating {
						res = m.execCallback(workerEC, task.data)
					}

				}, func(msg string) {
					m.errCallback(workerEC, errors.New(msg))
				}, terminateErrorIgnore)

				//根据返回类型的不同处理
				if res == nil {
					res = NormalResult[TaskType, ResultType]{
						rawTask: task,
					}
				} else {
					switch rt := res.(type) {
					case RetryResult[TaskType, ResultType]:
						rt.rawTask = task
						if len(rt.tasks) <= 0 {
							rt.tasks = []TaskType{task.data}
						}
						res = rt
					case NormalResult[TaskType, ResultType]:
						rt.rawTask = task
						res = rt
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
		collectorLogger := m.loggerInit(uint64(0), zerolog.New(nil)).
			With().
			Str("component", "collector").
			Int("thread_id", -1).
			Timestamp().
			Logger()
		collectorEC := m.ec.WithLogger(collectorLogger).WithContext(context.WithValue(m.ctx, "thread_id", uint64(0)))

		for ret := range resultQueue {
			if ret == nil {
				continue
			}

			if _, ok := ret.(NormalResult[TaskType, ResultType]); ok {
				if m.terminating {
					totalTaskWg.Done()
					continue
				}
				for _, rm := range m.resultMiddlewares {
					Try(func() {
						if currentNr, ok := ret.(NormalResult[TaskType, ResultType]); ok {
							ret = rm(collectorEC, currentNr.Data())
						}
					}, func(s string) {
						m.errCallback(collectorEC, errors.New(s))
					}, terminateErrorIgnore)
				}
			}

			switch rt := ret.(type) {
			case NormalResult[TaskType, ResultType]:
				m.totalResult += 1
				results = append(results, rt.Data())
				totalTaskWg.Done()

			case RetryResult[TaskType, ResultType]:
				tasks := rt.Tasks()
				if len(tasks) > 1 {
					totalTaskWg.Add(len(tasks) - 1)
				} else if len(tasks) == 0 {
					totalTaskWg.Done()
				}
				for _, rTask := range tasks {
					m.retry(rTask)
				}

			default:
				totalTaskWg.Done()
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

func newMultitasking[TaskType any, ResultType any](
	name string,
	inherit *Multitasking[TaskType, ResultType],
	debug bool,
) *Multitasking[TaskType, ResultType] {
	mt := &Multitasking[TaskType, ResultType]{
		name:      name,
		debug:     debug,
		inherit:   inherit,
		pauseChan: nil,
	}

	dc := NewBaseDistributeController[TaskType, ResultType]()
	ec := NewBaseExecuteController[TaskType, ResultType]()

	mt.SetController(dc)
	mt.SetController(ec)
	mt.SetLogger(nil)
	mt.SetErrorCallback(func(c Controller[TaskType, ResultType], err error) {
		//mt.Log(0, reflect.TypeOf(c).Name()+":"+err.Error())
	})
	return mt
}

// NewMultitasking 实例化一个多线程管理实例。如果需要嵌套，此实例应处于最上层。
func NewMultitasking[TaskType any, ResultType any](
	name string,
	inherit *Multitasking[TaskType, ResultType],
) *Multitasking[TaskType, ResultType] {
	lrm := newMultitasking[TaskType, ResultType](name, inherit, false)
	return lrm
}
