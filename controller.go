package Multitasking

import "context"

type Controller interface {
	Terminate()
	Status() MTStatus
	Protect(f func() error) error
	Name() string
	InheritDC() DistributeController
}

type DistributeController interface {
	Controller
	AddTask(any)
	AddTasks(...any)
}

type ExecuteController interface {
	Controller
	Retry(any)
	Context() context.Context
}

// BaseController 基础控制器，其他控制器都应当继承自此控制器
type BaseController struct {
	mt      *Multitasking
	inherit *Multitasking
}

func (bc *BaseController) Status() MTStatus {
	return bc.mt.status
}

func (bc *BaseController) Terminate() {
	bc.mt.terminate()
}

func (bc *BaseController) Name() string {
	return bc.mt.Name()
}

func (bc *BaseController) Protect(f func() error) error {
	return bc.mt.protect(f)
}

func (bc *BaseController) InheritDC() DistributeController {
	if bc.inherit != nil {
		return bc.inherit.dc
	} else {
		return nil
	}
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
		bdc.mt.addTask(task)
	}
}

// BaseExecuteController 基础的任务执行控制器
type BaseExecuteController struct {
	*BaseController
}

func (bec *BaseExecuteController) Retry(task any) {
	bec.mt.retry(task)
}

func (bec *BaseExecuteController) Context() context.Context {
	return bec.mt.ctx
}
