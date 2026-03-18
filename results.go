package Multitasking

type Result[TaskType any, ResultType any] interface {
	RawTask() Task[TaskType]
}

type RetryResult[TaskType any, ResultType any] struct {
	rawTask Task[TaskType]
	tasks   []TaskType
}

func (rr RetryResult[TaskType, ResultType]) RawTask() Task[TaskType] {
	return rr.rawTask
}

func (rr RetryResult[TaskType, ResultType]) Tasks() []TaskType {
	return rr.tasks
}

type NormalResult[TaskType any, ResultType any] struct {
	rawTask Task[TaskType]
	data    ResultType
}

func (nr NormalResult[TaskType, ResultType]) RawTask() Task[TaskType] {
	return nr.rawTask
}

func (nr NormalResult[TaskType, ResultType]) Data() ResultType {
	return nr.data
}

type NullResult[TaskType any, ResultType any] struct{}

func (null NullResult[TaskType, ResultType]) RawTask() Task[TaskType] {
	return Task[TaskType]{}
}
