package connector

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"time"
)

type Options struct {
	Url          string
	AckWait      time.Duration
	PublishTTL   time.Duration
	MaxReconnect int
	Name         string
}

func Connect(ctx context.Context, options Options) (*nats.Conn, error) {
	nc, err := nats.Connect(
		options.Url,
	)

	if err != nil {
		return nil, err
	}

	go func() {
		select {
		case <-ctx.Done():
			nc.Close()
		}
	}()

	return nc, nil
}

func JetStream(ctx context.Context, options Options) (nats.JetStreamContext, error) {
	nc, err := nats.Connect(options.Url)
	if err != nil {
		return nil, errors.New("can not connect to nats, " + err.Error())
	}

	go func() {
		select {
		case <-ctx.Done():
			nc.Close()
		}
	}()

	js, _ := nc.JetStream()
	return js, nil
}
