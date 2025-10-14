package publisher

import "time"

type PublishOptions struct {
	MsgID   string
	Headers map[string]string
	Timeout time.Duration
	Encoder func(v any) ([]byte, error)
}

type PublishOption func(*PublishOptions)

func WithMsgID(id string) PublishOption {
	return func(o *PublishOptions) {
		o.MsgID = id
	}
}

func WithHeaders(h map[string]string) PublishOption {
	return func(o *PublishOptions) {
		o.Headers = h
	}
}

func WithTimeout(d time.Duration) PublishOption {
	return func(o *PublishOptions) {
		o.Timeout = d
	}
}

func WithEncoder(enc func(v any) ([]byte, error)) PublishOption {
	return func(o *PublishOptions) {
		o.Encoder = enc
	}
}
