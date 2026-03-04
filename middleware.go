package Multitasking

type Middleware[TaskType any] func(ExecuteController[TaskType], TaskType) TaskType
