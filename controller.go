package Multitasking

import (
	"context"
)

type Controller interface {
	Terminate()
	Protect(f func()) error
	Name() string
	Debug(bool)
	Context() context.Context
	InheritDC() DistributeController
	Init(*Multitasking)
	Pause()
	Resume()
}

type DistributeController interface {
	Controller
	AddTask(any)
	AddTasks(...any)
}

type ExecuteController interface {
	Controller
	Retry(...any) RetryResult
}

type MiddlewareController interface {
	Controller
}

// BaseController 基础控制器，其他控制器都应当继承自此控制器
type BaseController struct {
	mt *Multitasking
}

func (bc *BaseController) Name() string {
	return bc.mt.Name()
}

func (bc *BaseController) Protect(f func()) error {
	return bc.mt.protect(f)
}

func (bc *BaseController) Pause() {
	bc.mt.pause()
}

func (bc *BaseController) Resume() {
	bc.mt.resume()
}

func (bc *BaseController) Debug(d bool) {
	bc.mt.debug = d
}

func (bc *BaseController) InheritDC() DistributeController {
	if bc.mt.inherit != nil {
		return bc.mt.inherit.dc
	} else {
		return nil
	}
}

func (bc *BaseController) Init(mt *Multitasking) {
	bc.mt = mt
}

func (bc *BaseController) Context() context.Context {
	return bc.mt.ctx
}

func NewBaseController() *BaseController {
	return &BaseController{}
}

// BaseDistributeController 基础的任务分发控制器
type BaseDistributeController struct {
	*BaseController
}

func (bdc *BaseDistributeController) AddTask(task any) {
	bdc.mt.addTask(task)
}

func (bdc *BaseDistributeController) AddTasks(tasks ...any) {
	for _, task := range tasks {
		bdc.AddTask(task)
	}
}

func (bdc *BaseDistributeController) Terminate() {
	//fmt.Println("TERMINATED")
	bdc.mt.terminating = true
	panic("multitasking terminated")
}

func NewBaseDistributeController() *BaseDistributeController {
	return &BaseDistributeController{
		NewBaseController(),
	}
}

// BaseExecuteController 基础的任务执行控制器
type BaseExecuteController struct {
	*BaseController
}

func (bec *BaseExecuteController) Retry(tasks ...any) RetryResult {
	return RetryResult{
		tasks: tasks,
	}
}

func (bec *BaseExecuteController) Terminate() {
	defer func() {
		if r := recover(); r != nil {
			//fmt.Println("Terminate:", r)
		}
	}()

	bec.mt.terminating = true
	TryClose(bec.mt.taskQueue)
	//TryClose(bec.mt.retryQueue.In)

}

func NewBaseExecuteController() *BaseExecuteController {
	return &BaseExecuteController{
		NewBaseController(),
	}
}
