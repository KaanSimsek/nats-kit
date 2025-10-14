package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
)

type Publisher interface {
	Publish(ctx context.Context, subject string, payload any, opts ...PublishOption) error
}

type publisher struct {
	js nats.JetStreamContext
}

func New(js nats.JetStreamContext) Publisher {
	return &publisher{
		js: js,
	}
}

func (p *publisher) Publish(ctx context.Context, subject string, v any, opts ...PublishOption) error {
	options := PublishOptions{
		Headers: map[string]string{},
		Encoder: json.Marshal,
	}

	for _, opt := range opts {
		opt(&options)
	}

	if options.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, options.Timeout)
		defer cancel()
	}

	data, err := options.Encoder(v)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{},
	}
	for k, v := range options.Headers {
		msg.Header.Set(k, v)
	}

	var pubOpts []nats.PubOpt
	if options.MsgID != "" {
		pubOpts = append(pubOpts, nats.MsgId(options.MsgID))
	}

	_, err = p.js.PublishMsg(msg, pubOpts...)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}
