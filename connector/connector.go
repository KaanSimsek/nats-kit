package connector

import (
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

func Connect(options Options) (*nats.Conn, error) {
	nc, err := nats.Connect(
		options.Url,
	)
	if err != nil {
		return nil, err
	}
	return nc, nil
}
