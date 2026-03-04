package Multitasking

import (
	"context"
)

type Controller[TaskType any] interface {
	Terminate()
	Protect(f func()) error
	Name() string
	Debug(bool)
	Context() context.Context
	InheritDC() DistributeController[TaskType]
	Init(*Multitasking[TaskType])
	Pause()
	Resume()
}

type DistributeController[TaskType any] interface {
	Controller[TaskType]
	AddTask(any)
	AddTasks(...any)
}

type ExecuteController[TaskType any] interface {
	Controller[TaskType]
	Retry(...any) RetryResult[TaskType]
}

type MiddlewareController[TaskType any] interface {
	Controller[TaskType]
}

// BaseController 基础控制器，其他控制器都应当继承自此控制器
type BaseController[TaskType any] struct {
	mt *Multitasking[TaskType]
}

func (bc *BaseController[TaskType]) Name() string {
	return bc.mt.Name()
}

func (bc *BaseController[TaskType]) Protect(f func()) error {
	return bc.mt.protect(f)
}

func (bc *BaseController[TaskType]) Pause() {
	bc.mt.pause()
}

func (bc *BaseController[TaskType]) Resume() {
	bc.mt.resume()
}

func (bc *BaseController[TaskType]) Debug(d bool) {
	bc.mt.debug = d
}

func (bc *BaseController[TaskType]) InheritDC() DistributeController[TaskType] {
	if bc.mt.inherit != nil {
		return bc.mt.inherit.dc
	} else {
		return nil
	}
}

func (bc *BaseController[TaskType]) Init(mt *Multitasking[TaskType]) {
	bc.mt = mt
}

func (bc *BaseController[TaskType]) Context() context.Context {
	return bc.mt.ctx
}

func NewBaseController[TaskType any]() *BaseController[TaskType] {
	return &BaseController[TaskType]{}
}

// BaseDistributeController 基础的任务分发控制器
type BaseDistributeController[TaskType any] struct {
	*BaseController[TaskType]
}

func (bdc *BaseDistributeController[TaskType]) AddTask(task TaskType) {
	bdc.mt.addTask(task)
}

func (bdc *BaseDistributeController[TaskType]) AddTasks(tasks ...TaskType) {
	for _, task := range tasks {
		bdc.AddTask(task)
	}
}

func (bdc *BaseDistributeController[TaskType]) Terminate() {
	//fmt.Println("TERMINATED")
	bdc.mt.terminating = true
	panic("multitasking terminated")
}

func NewBaseDistributeController[TaskType any]() *BaseDistributeController[TaskType] {
	return &BaseDistributeController[TaskType]{
		NewBaseController[TaskType](),
	}
}

// BaseExecuteController 基础的任务执行控制器
type BaseExecuteController[TaskType any] struct {
	*BaseController[TaskType]
}

func (bec *BaseExecuteController[TaskType]) Retry(
	tasks ...TaskType,
) RetryResult[TaskType] {
	return RetryResult[TaskType]{
		tasks: tasks,
	}
}

func (bec *BaseExecuteController[TaskType]) Terminate() {
	defer func() {
		if r := recover(); r != nil {
			//fmt.Println("Terminate:", r)
		}
	}()

	bec.mt.terminating = true
	TryClose(bec.mt.taskQueue)
	//TryClose(bec.mt.retryQueue.In)

}

func NewBaseExecuteController[TaskType any]() *BaseExecuteController[TaskType] {
	return &BaseExecuteController[TaskType]{
		NewBaseController[TaskType](),
	}
}
