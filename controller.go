package Multitasking

import "context"

type Controller interface {
	Terminate()
	Protect(f func()) error
	Name() string
	Debug(bool)
	Context() context.Context
	InheritDC() DistributeController
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
	mt      *Multitasking
	inherit *Multitasking
}

func (bc *BaseController) Name() string {
	return bc.mt.Name()
}

func (bc *BaseController) Protect(f func()) error {
	return bc.mt.protect(f)
}

func (bc *BaseController) Debug(d bool) {
	bc.mt.debug = d
}

func (bc *BaseController) InheritDC() DistributeController {
	if bc.inherit != nil {
		return bc.inherit.dc
	} else {
		return nil
	}
}

func (bc *BaseController) Context() context.Context {
	return bc.mt.ctx
}

func NewBaseController(mt, inherit *Multitasking) *BaseController {
	return &BaseController{
		mt:      mt,
		inherit: inherit,
	}
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
	panic("multitasking terminated")
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

	TryClose(bec.mt.taskQueue)
	//TryClose(bec.mt.retryQueue.In)

}
