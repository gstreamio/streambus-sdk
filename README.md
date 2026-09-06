# StreamBus Go SDK

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Go Reference](https://pkg.go.dev/badge/github.com/gstreamio/streambus-sdk.svg)](https://pkg.go.dev/github.com/gstreamio/streambus-sdk)
[![CI](https://github.com/gstreamio/streambus-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/gstreamio/streambus-sdk/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![GitHub release](https://img.shields.io/github/release/gstreamio/streambus-sdk.svg)](https://github.com/gstreamio/streambus-sdk/releases)

> 🚀 **High-performance Go SDK for StreamBus** - A modern, lightweight client library for building distributed streaming applications with [StreamBus](https://github.com/gstreamio/streambus).

StreamBus SDK provides a robust and efficient way to integrate Go applications with StreamBus, offering high-throughput message production, flexible consumption patterns, and enterprise-grade features like transactions and security.

## 📋 Table of Contents

- [Features](#-features)
- [Requirements](#-requirements)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Configuration](#️-configuration)
- [Advanced Features](#-advanced-features)
- [API Reference](#-api-reference)
- [Examples](#-examples)
- [Performance](#-performance)
- [Contributing](#-contributing)
- [Support](#-support)
- [License](#-license)

## ✨ Features

- **Simple Client API** - Easy-to-use client for producing and consuming messages
- **Producer Support** - Batched production with configurable acknowledgement
- **Consumer Support** - Partition consumers and coordinator-backed consumer groups
- **Transactional Support** - Exactly-once semantics with transactional producers and consumers
- **Connection Pooling** - Efficient connection management with configurable pooling
- **Security** - TLS/mTLS and SASL authentication support
- **Protocol Optimized** - High-performance binary protocol with minimal overhead
- **Zero Dependencies** - The SDK's published packages import only the Go standard library

## 📦 Requirements

- Go 1.26 or higher
- StreamBus broker v2.0+ running and accessible
- Network connectivity to StreamBus brokers

## 🚀 Installation

Install the SDK using Go modules:

```bash
go get github.com/gstreamio/streambus-sdk
```

## 🎯 Quick Start

Every call that performs network I/O takes a `context.Context`, so timeouts and
cancellation propagate all the way to the connection.

### Basic Producer

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/gstreamio/streambus-sdk/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	config := client.DefaultConfig()
	config.Brokers = []string{"localhost:9092"}

	c, err := client.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Create topic: 3 partitions, replication factor 1
	if err := c.CreateTopic(ctx, "events", 3, 1); err != nil {
		log.Printf("Topic creation: %v", err)
	}

	producer := client.NewProducer(c)
	defer producer.Close()

	if err := producer.Send(ctx, "events", []byte("key1"), []byte("Hello, StreamBus!")); err != nil {
		log.Fatal(err)
	}

	// The producer batches; flush before exiting so the batch is sent.
	if err := producer.Flush(ctx, "events"); err != nil {
		log.Fatal(err)
	}

	log.Println("Message sent successfully!")
}
```

### Basic Consumer

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/gstreamio/streambus-sdk/client"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	config := client.DefaultConfig()
	config.Brokers = []string{"localhost:9092"}

	c, err := client.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Consumer for topic "events", partition 0
	consumer := client.NewConsumer(c, "events", 0)
	defer consumer.Close()

	// Start from the beginning of the partition
	if err := consumer.Seek(0); err != nil {
		log.Fatal(err)
	}

	// Fetch returns a batch; FetchOne returns a single message.
	messages, err := consumer.Fetch(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for _, msg := range messages {
		log.Printf("Received: key=%s, value=%s, offset=%d",
			string(msg.Key), string(msg.Value), msg.Offset)
	}
}
```

### Consumer Group

A group consumer joins through the broker's group coordinator, receives a
partition assignment, heartbeats to hold its membership, and commits offsets
back to the coordinator.

```go
package main

import (
	"context"
	"log"

	"github.com/gstreamio/streambus-sdk/client"
)

func main() {
	ctx := context.Background()

	config := client.DefaultConfig()
	config.Brokers = []string{"localhost:9092"}

	c, err := client.New(config)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	groupConfig := client.DefaultGroupConsumerConfig()
	groupConfig.GroupID = "my-consumer-group"
	groupConfig.Topics = []string{"events"}

	consumer, err := client.NewGroupConsumer(c, groupConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// Subscribe joins the group and blocks until an assignment arrives.
	if err := consumer.Subscribe(ctx); err != nil {
		log.Fatal(err)
	}
	log.Printf("Assignment: %v", consumer.Assignment())

	for {
		batches, err := consumer.Poll(ctx)
		if err != nil {
			log.Fatal(err)
		}

		offsets := make(map[string]map[int32]int64)
		for topic, partitions := range batches {
			for partition, messages := range partitions {
				for _, msg := range messages {
					log.Printf("Group consumed: key=%s, value=%s",
						string(msg.Key), string(msg.Value))
				}
				if len(messages) == 0 {
					continue
				}
				if offsets[topic] == nil {
					offsets[topic] = make(map[int32]int64)
				}
				// Resume one past the last message processed.
				offsets[topic][partition] = messages[len(messages)-1].Offset + 1
			}
		}

		if len(offsets) > 0 {
			if err := consumer.CommitSync(ctx, offsets); err != nil {
				log.Printf("Commit failed: %v", err)
			}
		}
	}
}
```

## ⚙️ Configuration

### Client Configuration

```go
config := &client.Config{
	Brokers:        []string{"localhost:9092"},
	ConnectTimeout: 10 * time.Second,
	ReadTimeout:    30 * time.Second,
	WriteTimeout:   30 * time.Second,
	RequestTimeout: 30 * time.Second,

	// Connection pooling
	MaxConnectionsPerBroker: 5,
	KeepAlive:               true,
	KeepAlivePeriod:         30 * time.Second,

	// Retry configuration
	MaxRetries:    3,
	RetryBackoff:  100 * time.Millisecond,
	RetryMaxDelay: 30 * time.Second,
}
```

`client.DefaultConfig()` returns this same shape already populated, including
nested `ProducerConfig` and `ConsumerConfig` defaults.

### TLS Configuration

```go
config := client.DefaultConfig()
config.Security = &client.SecurityConfig{
	TLS: &client.TLSConfig{
		Enabled:    true,
		CAFile:     "/path/to/ca.crt",
		CertFile:   "/path/to/client.crt", // For mTLS
		KeyFile:    "/path/to/client.key", // For mTLS
		ServerName: "streambus.example.com",
	},
}
```

### SASL Authentication

```go
config := client.DefaultConfig()
config.Security = &client.SecurityConfig{
	SASL: &client.SASLConfig{
		Enabled:   true,
		Mechanism: "SCRAM-SHA-256",
		Username:  "producer1",
		Password:  "secure-password",
	},
}
```

## 🔧 Advanced Features

### Transactional Producer

Messages sent inside a transaction become visible to `ReadCommitted` consumers
all at once on commit, or never, if the transaction aborts.

```go
txnConfig := client.DefaultTransactionalProducerConfig()
txnConfig.TransactionID = "my-transaction"

producer, err := client.NewTransactionalProducer(c, txnConfig)
if err != nil {
	log.Fatal(err)
}
defer producer.Close()

if err := producer.BeginTransaction(ctx); err != nil {
	log.Fatal(err)
}

msg := protocol.Message{
	Key:       []byte("key"),
	Value:     []byte("value"),
	Timestamp: time.Now().UnixNano(),
}

// Send targets an explicit partition.
if err := producer.Send(ctx, "events", 0, msg); err != nil {
	if abortErr := producer.AbortTransaction(ctx); abortErr != nil {
		log.Printf("Abort failed: %v", abortErr)
	}
	log.Fatal(err)
}

if err := producer.CommitTransaction(ctx); err != nil {
	log.Fatal(err)
}
```

To tie consumption and production into one atomic unit (consume-transform-produce),
pass the consumer's offsets into the transaction with
`producer.SendOffsetsToTransaction(ctx, groupID, offsets)` before committing.

### Transactional Consumer

```go
consumerConfig := client.DefaultTransactionalConsumerConfig()
consumerConfig.Client = c
consumerConfig.Topics = []string{"events"}
consumerConfig.IsolationLevel = client.ReadCommitted

consumer, err := client.NewTransactionalConsumer(consumerConfig)
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

// Only returns messages from committed transactions.
records, err := consumer.Poll(ctx)
if err != nil {
	log.Fatal(err)
}

for _, record := range records {
	log.Printf("%s[%d]@%d: %s",
		record.Topic, record.Partition, record.Offset, record.Message.Value)
}
```

## 📚 API Reference

### Client

- `New(config *Config) (*Client, error)` - Create a new client
- `CreateTopic(ctx context.Context, topic string, partitions uint32, replicationFactor uint16) error`
- `DeleteTopic(ctx context.Context, topic string) error`
- `ListTopics(ctx context.Context) ([]string, error)`
- `Close() error` - Close the client and all connections

### Producer

- `NewProducer(client *Client) *Producer` - Create a new producer
- `Send(ctx context.Context, topic string, key, value []byte) error`
- `SendToPartition(ctx context.Context, topic string, partition uint32, key, value []byte) error`
- `Flush(ctx context.Context, topic string) error` / `FlushAll(ctx context.Context) error`
- `Close() error` - Flush and close the producer

### Consumer

- `NewConsumer(client *Client, topic string, partition uint32) *Consumer`
- `Seek(offset int64) error` / `SeekToBeginning() error` / `SeekToEnd(ctx context.Context) error`
- `Fetch(ctx context.Context) ([]protocol.Message, error)` - Fetch a batch
- `FetchOne(ctx context.Context) (*protocol.Message, error)` - Fetch a single message
- `Poll(ctx context.Context, interval time.Duration, handler func([]protocol.Message) error) error`
- `Close() error`

### Group Consumer

- `NewGroupConsumer(client *Client, config GroupConsumerConfig) (*GroupConsumer, error)`
- `Subscribe(ctx context.Context) error` - Join the group and await an assignment
- `Poll(ctx context.Context) (map[string]map[int32][]protocol.Message, error)`
- `CommitSync(ctx context.Context, offsets map[string]map[int32]int64) error`
- `Committed(ctx context.Context) (map[string]map[int32]int64, error)`
- `Assignment() map[string][]int32` - Current partition assignment
- `SetRebalanceListener(listener RebalanceListener)`
- `Close() error` - Leave the group and close

### Transactional Producer

- `NewTransactionalProducer(client *Client, config TransactionalProducerConfig) (*TransactionalProducer, error)`
- `BeginTransaction(ctx context.Context) error`
- `Send(ctx context.Context, topic string, partition int32, message protocol.Message) error`
- `SendOffsetsToTransaction(ctx context.Context, groupID string, offsets map[string]map[int32]int64) error`
- `CommitTransaction(ctx context.Context) error` / `AbortTransaction(ctx context.Context) error`

## 💡 Examples

See the [examples directory](./examples) for complete working programs:

- [Basic Producer/Consumer](./examples/basic) - Simple message production and consumption
- [Consumer Group](./examples/consumergroup) - Coordinated consumption with offset commits
- [Transactions](./examples/transaction) - Atomic commit and abort

## ⚡ Performance

1. **Connection Pooling**: Configure appropriate pool sizes for your workload
2. **Batching**: Tune `ProducerConfig.BatchSize` and `BatchTimeout` for your latency budget
3. **Partition Strategy**: Distribute load across multiple partitions
4. **Consumer Groups**: Scale consumers horizontally with consumer groups
5. **Keep-Alive**: Enable TCP keep-alive for long-lived connections

Benchmarks ship with the packages themselves; run them against your own
hardware rather than relying on quoted figures:

```bash
go test -bench=. -benchmem ./...
```

## 🛠️ Error Handling

The SDK uses standard Go error handling patterns with typed errors for common scenarios:

```go
if err := producer.Send(ctx, "topic", key, value); err != nil {
	switch {
	case errors.Is(err, client.ErrAllBrokersFailed):
		// Every broker was unreachable
	case errors.Is(err, client.ErrRequestTimeout):
		// Handle timeouts
	case errors.Is(err, client.ErrInvalidTopic):
		// Handle invalid topic
	default:
		// Handle other errors
	}
}
```

See [`client/errors.go`](./client/errors.go) for the full set.

## 🤝 Contributing

We welcome contributions! Please read our [Contributing Guidelines](./CONTRIBUTING.md) to get started.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/gstreamio/streambus-sdk.git
cd streambus-sdk

# Build
go build ./...

# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./...

# Lint (matches CI)
golangci-lint run ./...
```

Tests here cover the SDK packages in isolation. The end-to-end tests that need
a live broker live in the [StreamBus repository](https://github.com/gstreamio/streambus)
alongside the server they exercise.

## 📖 Documentation

- **[API Documentation](https://gstreamio.github.io/streambus-sdk/)** - Complete API reference
- **[Getting Started Guide](https://gstreamio.github.io/streambus-sdk/getting-started)** - Step-by-step tutorial
- **[Architecture Overview](https://gstreamio.github.io/streambus-sdk/architecture)** - SDK design and internals
- **[Best Practices](https://gstreamio.github.io/streambus-sdk/best-practices)** - Production deployment guidelines

## 📞 Support

- 📚 **Documentation**: [StreamBus Docs](https://gstreamio.github.io/streambus-sdk/)
- 🐛 **Issues**: [GitHub Issues](https://github.com/gstreamio/streambus-sdk/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/gstreamio/streambus-sdk/discussions)
- 🏠 **StreamBus Broker**: [github.com/gstreamio/streambus](https://github.com/gstreamio/streambus)

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](./LICENSE) file for details.

---

<div align="center">
Built with ❤️ by the StreamBus team | <a href="https://github.com/gstreamio/streambus-sdk">Star us on GitHub</a>
</div>
