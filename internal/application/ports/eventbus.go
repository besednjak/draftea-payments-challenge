package ports

import (
	"context"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
)

// EventHandler processes a single delivered Envelope within a consumer group.
type EventHandler func(ctx context.Context, env events.Envelope) error

// DeadLetter preserves a message and its failure metadata after exhausting retries.
type DeadLetter struct {
	OriginalTopic string
	PartitionKey  string
	ConsumerGroup string
	Error         string
	Attempts      int
	FirstFailedAt time.Time
	Envelope      events.Envelope
}

// EventPublisher publishes commands or domain events partitioned by key.
type EventPublisher interface {
	Publish(ctx context.Context, topic, partitionKey string, env events.Envelope) error
}

// EventSubscriber registers consumer-group handlers on a topic.
type EventSubscriber interface {
	Subscribe(topic, consumerGroup string, handler EventHandler) error
}

// EventBus combines publishing, subscribing, and graceful draining.
type EventBus interface {
	EventPublisher
	EventSubscriber
	Close(ctx context.Context) error
}

// DLQReader allows operational tooling, alerts, and tests to inspect dead-lettered messages.
type DLQReader interface {
	DeadLetters(topic string) []DeadLetter
}
