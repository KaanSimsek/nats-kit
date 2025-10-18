package consumer

import "time"

type SubscribeOptions struct {
	AutoAck bool
	Backoff time.Duration
	Decoder func([]byte, any) error
}

type SubscribeOption func(*SubscribeOptions)

func WithAutoAck(enabled bool) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.AutoAck = enabled
	}
}

func WithBackoff(d time.Duration) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Backoff = d
	}
}

func WithDecoder(fn func([]byte, any) error) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Decoder = fn
	}
}
