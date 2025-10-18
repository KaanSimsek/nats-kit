package consumer

import "context"

type Handler[T any] interface {
	Handle(ctx context.Context, msg T) error
}

type HandlerFunc[T any] func(ctx context.Context, msg *T) error

func (f HandlerFunc[T]) Handle(ctx context.Context, msg *T) error {
	return f(ctx, msg)
}
