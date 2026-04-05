package publisher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type RedpandaPublisher struct {
	client *kgo.Client
}

func NewRedpandaPublisher(brokers []string) (*RedpandaPublisher, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("brokers must not be empty")
	}
	for _, broker := range brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, fmt.Errorf("brokers contains empty address")
		}
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string { return "" })),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &RedpandaPublisher{client: client}, nil
}

func (p *RedpandaPublisher) Publish(ctx context.Context, topic string, key, value []byte) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	err := p.client.ProduceSync(ctx, record).FirstErr()
	if err != nil {
		return fmt.Errorf("failed to produce record: %w", err)
	}

	return nil
}

func (p *RedpandaPublisher) Close() error {
	p.client.Close()
	return nil
}
