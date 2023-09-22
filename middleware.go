package Multitasking

type Middleware interface {
	Run(ExecuteController, interface{}) interface{}
}

type BaseMiddleware struct {
	f   func(ExecuteController, interface{}) (interface{}, error)
	err error
}

func (bm BaseMiddleware) Run(ec ExecuteController, i interface{}) interface{} {
	var res interface{}
	res, bm.err = bm.f(ec, i)
	return res
}

func (bm BaseMiddleware) Error() error {
	return bm.err
}

// NewBaseMiddleware 实例化一个基础的Middleware
func NewBaseMiddleware(f func(ExecuteController, interface{}) (interface{}, error)) *BaseMiddleware {
	return &BaseMiddleware{
		f: f,
	}
}
