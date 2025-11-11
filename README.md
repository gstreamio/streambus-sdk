# StreamBus Go SDK

The official Go SDK for [StreamBus](https://github.com/shawntherrien/streambus), a high-performance distributed streaming platform.

## Features

- **Simple Client API** - Easy-to-use client for producing and consuming messages
- **Producer Support** - Synchronous and asynchronous message production
- **Consumer Support** - Simple partition consumers and consumer groups
- **Transactional Support** - Exactly-once semantics with transactional producers and consumers
- **Connection Pooling** - Efficient connection management with configurable pooling
- **Security** - TLS/mTLS and SASL authentication support
- **Protocol Optimized** - High-performance binary protocol with minimal overhead

## Installation

```bash
go get github.com/stherrien/streambus-sdk
```

## Quick Start

### Basic Producer

```go
package main

import (
    "log"
    "github.com/stherrien/streambus-sdk/client"
)

func main() {
    // Create client configuration
    config := client.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    // Create client
    c, err := client.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Create topic
    if err := c.CreateTopic("events", 3, 1); err != nil {
        log.Printf("Topic creation: %v", err)
    }

    // Create producer
    producer := client.NewProducer(c)
    defer producer.Close()

    // Send message
    key := []byte("key1")
    value := []byte("Hello, StreamBus!")

    if err := producer.Send("events", key, value); err != nil {
        log.Fatal(err)
    }

    log.Println("Message sent successfully!")
}
```

### Basic Consumer

```go
package main

import (
    "log"
    "github.com/stherrien/streambus-sdk/client"
)

func main() {
    // Create client
    config := client.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    c, err := client.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Create consumer for topic "events", partition 0
    consumer := client.NewConsumer(c, "events", 0)
    defer consumer.Close()

    // Set starting offset
    if err := consumer.Seek(0); err != nil {
        log.Fatal(err)
    }

    // Fetch messages
    for i := 0; i < 10; i++ {
        record, err := consumer.Fetch()
        if err != nil {
            log.Printf("Fetch error: %v", err)
            continue
        }

        log.Printf("Received: key=%s, value=%s, offset=%d",
            string(record.Key), string(record.Value), record.Offset)
    }
}
```

### Consumer Group

```go
package main

import (
    "context"
    "log"
    "time"
    "github.com/stherrien/streambus-sdk/client"
)

func main() {
    config := client.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    c, err := client.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Create group consumer
    groupConfig := &client.GroupConsumerConfig{
        GroupID: "my-consumer-group",
        Topics:  []string{"events"},
    }

    consumer, err := client.NewGroupConsumer(c, groupConfig)
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    // Start consuming
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    if err := consumer.Start(ctx, func(record *client.Record) error {
        log.Printf("Group consumed: key=%s, value=%s",
            string(record.Key), string(record.Value))
        return nil
    }); err != nil {
        log.Fatal(err)
    }
}
```

## Configuration

### Client Configuration

```go
config := &client.Config{
    Brokers:        []string{"localhost:9092"},
    ConnectTimeout: 10 * time.Second,
    RequestTimeout: 30 * time.Second,

    // Connection pooling
    MaxIdleConns:    10,
    MaxConnsPerHost: 100,
    IdleConnTimeout: 90 * time.Second,

    // Retry configuration
    MaxRetries:  3,
    RetryBackoff: 100 * time.Millisecond,
}
```

### TLS Configuration

```go
config := client.DefaultConfig()
config.Security = &client.SecurityConfig{
    TLS: &client.TLSConfig{
        Enabled:    true,
        CAFile:     "/path/to/ca.crt",
        CertFile:   "/path/to/client.crt",  // For mTLS
        KeyFile:    "/path/to/client.key",   // For mTLS
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

## Advanced Features

### Transactional Producer

```go
config := &client.TransactionalProducerConfig{
    TransactionID: "my-transaction",
}

producer, err := client.NewTransactionalProducer(c, config)
if err != nil {
    log.Fatal(err)
}
defer producer.Close()

// Begin transaction
if err := producer.BeginTransaction(); err != nil {
    log.Fatal(err)
}

// Send messages
if err := producer.Send("events", []byte("key"), []byte("value")); err != nil {
    producer.AbortTransaction()
    log.Fatal(err)
}

// Commit transaction
if err := producer.CommitTransaction(); err != nil {
    log.Fatal(err)
}
```

### Transactional Consumer

```go
config := &client.TransactionalConsumerConfig{
    Topic:          "events",
    Partition:      0,
    IsolationLevel: client.ReadCommitted,
}

consumer, err := client.NewTransactionalConsumer(c, config)
if err != nil {
    log.Fatal(err)
}
defer consumer.Close()

// Only reads committed messages
record, err := consumer.Fetch()
if err != nil {
    log.Fatal(err)
}
```

## API Reference

### Client

- `New(config *Config) (*Client, error)` - Create a new client
- `CreateTopic(name string, partitions, replicas int) error` - Create a topic
- `DeleteTopic(name string) error` - Delete a topic
- `ListTopics() ([]string, error)` - List all topics
- `Close() error` - Close the client and all connections

### Producer

- `NewProducer(client *Client) *Producer` - Create a new producer
- `Send(topic string, key, value []byte) error` - Send a message
- `Close() error` - Close the producer

### Consumer

- `NewConsumer(client *Client, topic string, partition int) *Consumer` - Create partition consumer
- `Seek(offset int64) error` - Set the starting offset
- `Fetch() (*Record, error)` - Fetch the next message
- `Close() error` - Close the consumer

### Group Consumer

- `NewGroupConsumer(client *Client, config *GroupConsumerConfig) (*GroupConsumer, error)` - Create group consumer
- `Start(ctx context.Context, handler MessageHandler) error` - Start consuming with handler
- `Close() error` - Close the consumer and leave the group

## Examples

See the [examples directory](./examples) for more detailed examples:

- [Basic Producer/Consumer](./examples/basic)
- [Consumer Groups](./examples/consumer-group)
- [Transactional Messaging](./examples/transactions)
- [Secure Connections](./examples/secure)

## Performance Tips

1. **Connection Pooling**: Configure appropriate pool sizes for your workload
2. **Batching**: Use transactional producers for batching multiple messages
3. **Partition Strategy**: Distribute load across multiple partitions
4. **Consumer Groups**: Scale consumers horizontally with consumer groups
5. **Keep-Alive**: Enable TCP keep-alive for long-lived connections

## Error Handling

The SDK uses standard Go error handling patterns:

```go
if err := producer.Send("topic", key, value); err != nil {
    switch {
    case errors.Is(err, client.ErrConnectionFailed):
        // Handle connection errors
    case errors.Is(err, client.ErrTimeout):
        // Handle timeouts
    default:
        // Handle other errors
    }
}
```

## Contributing

Contributions are welcome! Please see the [main StreamBus repository](https://github.com/shawntherrien/streambus) for contribution guidelines.

## License

Apache License 2.0 - see LICENSE file for details

## Support

- **Documentation**: [StreamBus Docs](https://github.com/shawntherrien/streambus/docs)
- **Issues**: [GitHub Issues](https://github.com/stherrien/streambus-sdk/issues)
- **Community**: [Discussions](https://github.com/shawntherrien/streambus/discussions)
- **StreamBus Broker**: [github.com/shawntherrien/streambus](https://github.com/shawntherrien/streambus)
