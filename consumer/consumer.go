package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"log/slog"
	"time"
)

type Consumer interface {
	Subscribe(ctx context.Context, subject string, handler func(context.Context, []byte) error, opts ...SubscribeOption) error
}

type consumer struct {
	js     nats.JetStreamContext
	logger *slog.Logger
}

func NewConsumer(js nats.JetStreamContext) Consumer {
	return &consumer{
		js:     js,
		logger: slog.Default(),
	}
}

func (c *consumer) Subscribe(ctx context.Context, subject string, handler func(context.Context, []byte) error, opts ...SubscribeOption) error {
	if c.js == nil {
		return errors.New("JetStream context not initialized")
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}

	options := SubscribeOptions{
		AutoAck: true,
		Decoder: json.Unmarshal,
		Backoff: 0,
	}
	for _, opt := range opts {
		opt(&options)
	}

	sub, err := c.js.Subscribe(subject, func(m *nats.Msg) {
		if err := handler(ctx, m.Data); err != nil {
			c.logger.Error("handler failed", "subject", subject, "err", err)
			if options.Backoff > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(options.Backoff):
				}
			}
			_ = m.Nak()
			return
		}

		if options.AutoAck {
			_ = m.Ack()
		}
	})
	if err != nil {
		return fmt.Errorf("subscribe to %s: %w", subject, err)
	}

	// Graceful unsubscribe on context cancellation
	go func() {
		<-ctx.Done()
		_ = sub.Unsubscribe()
	}()

	return nil
}

// SubscribeTyped is a type-safe wrapper for Subscribe.
// It automatically decodes messages into type T before passing them to handler.
func SubscribeTyped[T any](ctx context.Context, c Consumer, subject string, h Handler[T], opts ...SubscribeOption) error {
	decoder := json.Unmarshal
	for _, o := range opts {
		o(&SubscribeOptions{Decoder: decoder})
	}

	return c.Subscribe(ctx, subject, func(ctx context.Context, data []byte) error {
		var msg T
		if err := decoder(data, &msg); err != nil {
			return fmt.Errorf("decode failed: %w", err)
		}
		return h.Handle(ctx, msg)
	}, opts...)
}
