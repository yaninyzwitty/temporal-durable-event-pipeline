package publisher

import (
	"context"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
)

type RedpandaPublisher struct {
	client *kgo.Client
}

func NewRedpandaPublisher(brokers []string) (*RedpandaPublisher, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string { return "" })),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &RedpandaPublisher{client: client}, nil
}

func (p *RedpandaPublisher) Publish(ctx context.Context, topic string, key, value []byte) error {
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
