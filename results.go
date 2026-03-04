package Multitasking

type Result[TaskType any] interface {
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

type NullResult[TaskType any] struct{}

func (null NullResult[TaskType]) RawTask() Task[TaskType] {
	return Task[TaskType]{}
}
