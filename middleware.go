package Multitasking

type Middleware[TaskType any] func(ExecuteController[TaskType], TaskType) Result[TaskType]
