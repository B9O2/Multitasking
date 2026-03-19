package Multitasking

type Middleware[TaskType any, ResultType any] func(ExecuteController[TaskType, ResultType], ThreadController, ResultType) Result[TaskType, ResultType]
