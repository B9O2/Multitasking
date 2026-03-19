package Multitasking

type Middleware[TaskType any, ResultType any] func(ExecuteController[TaskType, ResultType], ResultType) Result[TaskType, ResultType]
