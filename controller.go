package Multitasking

import (
	"context"
)

type Controller[TaskType any, ResultType any] interface {
	Terminate()
	Protect(f func()) error
	Name() string
	Debug(bool)
	Context() context.Context
	InheritDC() DistributeController[TaskType, ResultType]
	Init(*Multitasking[TaskType, ResultType])
	Pause()
	Resume()
}

type DistributeController[TaskType any, ResultType any] interface {
	Controller[TaskType, ResultType]
	AddTask(TaskType)
	AddTasks(...TaskType)
}

type ExecuteController[TaskType any, ResultType any] interface {
	Controller[TaskType, ResultType]
	Retry(...TaskType) Result[TaskType, ResultType]
	Success(ResultType) Result[TaskType, ResultType]
	Null() Result[TaskType, ResultType]
}

type MiddlewareController[TaskType any, ResultType any] interface {
	Controller[TaskType, ResultType]
}

// BaseController 基础控制器，其他控制器都应当继承自此控制器
type BaseController[TaskType any, ResultType any] struct {
	mt *Multitasking[TaskType, ResultType]
}

func (bc *BaseController[TaskType, ResultType]) Name() string {
	return bc.mt.Name()
}

func (bc *BaseController[TaskType, ResultType]) Protect(f func()) error {
	return bc.mt.protect(f)
}

func (bc *BaseController[TaskType, ResultType]) Pause() {
	bc.mt.pause()
}

func (bc *BaseController[TaskType, ResultType]) Resume() {
	bc.mt.resume()
}

func (bc *BaseController[TaskType, ResultType]) Debug(d bool) {
	bc.mt.debug = d
}

func (bc *BaseController[TaskType, ResultType]) InheritDC() DistributeController[TaskType, ResultType] {
	if bc.mt.inherit != nil {
		return bc.mt.inherit.dc
	} else {
		return nil
	}
}

func (bc *BaseController[TaskType, ResultType]) Init(mt *Multitasking[TaskType, ResultType]) {
	bc.mt = mt
}

func (bc *BaseController[TaskType, ResultType]) Context() context.Context {
	return bc.mt.ctx
}

func NewBaseController[TaskType any, ResultType any]() *BaseController[TaskType, ResultType] {
	return &BaseController[TaskType, ResultType]{}
}

// BaseDistributeController 基础的任务分发控制器
type BaseDistributeController[TaskType any, ResultType any] struct {
	*BaseController[TaskType, ResultType]
}

func (bdc *BaseDistributeController[TaskType, ResultType]) AddTask(task TaskType) {
	bdc.mt.addTask(task)
}

func (bdc *BaseDistributeController[TaskType, ResultType]) AddTasks(tasks ...TaskType) {
	for _, task := range tasks {
		bdc.AddTask(task)
	}
}

func (bdc *BaseDistributeController[TaskType, ResultType]) Terminate() {
	//fmt.Println("TERMINATED")
	bdc.mt.terminating = true
	panic("multitasking terminated")
}

func NewBaseDistributeController[TaskType any, ResultType any]() *BaseDistributeController[TaskType, ResultType] {
	return &BaseDistributeController[TaskType, ResultType]{
		NewBaseController[TaskType, ResultType](),
	}
}

// BaseExecuteController 基础的任务执行控制器
type BaseExecuteController[TaskType any, ResultType any] struct {
	*BaseController[TaskType, ResultType]
}

func (bec *BaseExecuteController[TaskType, ResultType]) Retry(
	tasks ...TaskType,
) Result[TaskType, ResultType] {
	return RetryResult[TaskType, ResultType]{
		tasks: tasks,
	}
}

func (bec *BaseExecuteController[TaskType, ResultType]) Null() Result[TaskType, ResultType] {
	return NullResult[TaskType, ResultType]{}
}

func (bec *BaseExecuteController[TaskType, ResultType]) Success(
	data ResultType,
) Result[TaskType, ResultType] {
	return NormalResult[TaskType, ResultType]{
		data: data,
	}
}

func (bec *BaseExecuteController[TaskType, ResultType]) Terminate() {
	defer func() {
		if r := recover(); r != nil {
			//fmt.Println("Terminate:", r)
		}
	}()

	bec.mt.terminating = true
	TryClose(bec.mt.taskQueue)
	//TryClose(bec.mt.retryQueue.In)

}

func NewBaseExecuteController[TaskType any, ResultType any]() *BaseExecuteController[TaskType, ResultType] {
	return &BaseExecuteController[TaskType, ResultType]{
		NewBaseController[TaskType, ResultType](),
	}
}
