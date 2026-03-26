# Multitasking Project - Agent Skills & Context

This document is written for AI Agents (like Trae, GitHub Copilot, etc.) to quickly understand the architecture, core components, and coding conventions of the `Multitasking` project.

## 1. Project Overview
- **Name**: Multitasking
- **Language**: Go (1.18+ requiring Generics)
- **Purpose**: A concurrency library that abstracts away thread scheduling, providing built-in support for task distribution, task execution, result collection, retries, exception handling, execution interruption, and result middlewares.
- **Core Paradigm**: The lifecycle of tasks is divided into **Distribution (任务分发)** and **Execution (任务执行)**.

## 2. Core Concepts & Generics
The library is heavily based on Go Generics: `[TaskType any, ResultType any]`.
Whenever you instantiate or implement interfaces for this library, you must strictly follow the generic types.

### 2.1 Multitasking Instance
Created via `NewMultitasking(name string, inherit *Multitasking)`.
- **`Register(distributeFunc, executeFunc)`**: Binds the distribution logic and execution logic.
- **`Run(ctx context.Context, threads uint64)`**: Blocks and runs the pool with the specified number of goroutines.

### 2.2 Controllers
Controllers are passed into callbacks to manage task state and lifecycle.
- **`DistributeController`**: 
  - Used in the task distribution phase.
  - **Methods**: `AddTask(TaskType)`, `AddTasks(...TaskType)`.
- **`ExecuteController`**: 
  - Used in the task execution phase.
  - **Methods**: 
    - `Success(ResultType)`: Returns a successful result.
    - `Retry(...TaskType)`: Re-queues the task(s) for a retry.
    - `Null()`: Aborts the current task without yielding a result.
- **`BaseController`**: 
  - The underlying implementation providing common context operations: `Context()`, `Logger()`, `Pause()`, `Resume()`, `Terminate()`, `Protect(func())`.

### 2.3 Results
The execution phase must return a `Result[TaskType, ResultType]`.
- **`NormalResult`**: Contains the successful data.
- **`RetryResult`**: Signals the task should be pushed to the retry queue.
- **`NullResult`**: Ignored in the final result collection.

## 3. Coding Guidelines & Agent Instructions

When you are asked to write code using or modifying this project, adhere to the following rules:

### 3.1 Use Provided Controllers for Returns
Do not return bare data in the execution function. Always wrap it using `ExecuteController` methods:
```go
// CORRECT
return ec.Success(data)
return ec.Retry(task)
return ec.Null()

// INCORRECT
return data // Will cause compilation errors
```

### 3.2 Context & Logging
Always use the context and logger bound to the current controller rather than global ones:
```go
// CORRECT
ctx := dc.Context()
logger := dc.Logger()

// INCORRECT
ctx := context.Background()
```

### 3.3 Panic Protection
When dealing with external unstable logic, use the controller's `Protect` method to gracefully recover from panics.
```go
ec.Protect(func() {
    // risky logic
})
```

### 3.4 Middlewares
Middlewares are defined as `func(ExecuteController, ResultType) Result`. They allow intercepting and modifying results before they are finalized. When adding middlewares, ensure they are registered properly on the `Multitasking` instance.

## 4. Quick Boilerplate Example
If the user asks you to implement a concurrent scraper or batch processor, use this structure:

```go
mt := Multitasking.NewMultitasking[string, int]("example", nil)

mt.Register(func(dc Multitasking.DistributeController[string, int]) {
    dc.AddTasks("task1", "task2", "task3")
}, func(ec Multitasking.ExecuteController[string, int], task string) Multitasking.Result[string, int] {
    // Do work
    if err != nil {
        return ec.Retry(task)
    }
    return ec.Success(1)
})

results, err := mt.Run(context.Background(), 10)
```
