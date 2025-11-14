---
layout: default
title: Getting Started with StreamBus SDK
---

# Getting Started

This guide will walk you through setting up and using the StreamBus Go SDK to build your first streaming application.

## Prerequisites

Before you begin, make sure you have:

- **Go 1.19 or higher** installed ([Download Go](https://golang.org/dl/))
- **StreamBus broker** running (see [StreamBus installation](https://github.com/shawntherrien/streambus))
- Basic knowledge of Go programming

## Installation

### Step 1: Create a New Go Module

```bash
mkdir my-streambus-app
cd my-streambus-app
go mod init my-streambus-app
```

### Step 2: Install the SDK

```bash
go get github.com/gstreamio/streambus-sdk
```

### Step 3: Import the SDK

```go
import "github.com/gstreamio/streambus-sdk/client"
```

## Your First Producer

Let's create a simple producer that sends messages to StreamBus:

### producer.go

```go
package main

import (
    "fmt"
    "log"
    "time"
    "github.com/gstreamio/streambus-sdk/client"
)

func main() {
    // Create configuration
    config := client.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    // Connect to StreamBus
    c, err := client.New(config)
    if err != nil {
        log.Fatal("Failed to connect:", err)
    }
    defer c.Close()

    // Create a topic (if it doesn't exist)
    if err := c.CreateTopic("my-events", 3, 1); err != nil {
        log.Printf("Topic creation: %v", err)
    }

    // Create a producer
    producer := client.NewProducer(c)
    defer producer.Close()

    // Send 10 messages
    for i := 0; i < 10; i++ {
        key := []byte(fmt.Sprintf("key-%d", i))
        value := []byte(fmt.Sprintf("Message #%d sent at %s", i, time.Now()))

        if err := producer.Send("my-events", key, value); err != nil {
            log.Printf("Failed to send message: %v", err)
            continue
        }

        fmt.Printf("✓ Sent message %d\n", i)
        time.Sleep(time.Second)
    }

    fmt.Println("Producer finished!")
}
```

### Running the Producer

```bash
go run producer.go
```

You should see output like:

```
✓ Sent message 0
✓ Sent message 1
✓ Sent message 2
...
Producer finished!
```

## Your First Consumer

Now let's create a consumer to read those messages:

### consumer.go

```go
package main

import (
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"
    "github.com/gstreamio/streambus-sdk/client"
)

func main() {
    // Create configuration
    config := client.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    // Connect to StreamBus
    c, err := client.New(config)
    if err != nil {
        log.Fatal("Failed to connect:", err)
    }
    defer c.Close()

    // Create consumer for partition 0
    consumer := client.NewConsumer(c, "my-events", 0)
    defer consumer.Close()

    // Start from the beginning
    if err := consumer.Seek(0); err != nil {
        log.Fatal("Failed to seek:", err)
    }

    // Set up signal handling for graceful shutdown
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

    fmt.Println("Consumer started. Press Ctrl+C to stop...")

    // Consume messages
    for {
        select {
        case <-signals:
            fmt.Println("\nShutting down...")
            return
        default:
            record, err := consumer.Fetch()
            if err != nil {
                log.Printf("Fetch error: %v", err)
                continue
            }

            fmt.Printf("📨 Received: key=%s, value=%s, offset=%d\n",
                string(record.Key), string(record.Value), record.Offset)
        }
    }
}
```

### Running the Consumer

```bash
go run consumer.go
```

You should see the messages being consumed:

```
Consumer started. Press Ctrl+C to stop...
📨 Received: key=key-0, value=Message #0 sent at 2024-01-15 10:30:00, offset=0
📨 Received: key=key-1, value=Message #1 sent at 2024-01-15 10:30:01, offset=1
...
```

## Using Consumer Groups

Consumer groups allow multiple consumers to work together to process messages:

### group_consumer.go

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"
    "github.com/gstreamio/streambus-sdk/client"
)

func main() {
    // Create configuration
    config := client.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    // Connect to StreamBus
    c, err := client.New(config)
    if err != nil {
        log.Fatal("Failed to connect:", err)
    }
    defer c.Close()

    // Configure consumer group
    groupConfig := &client.GroupConsumerConfig{
        GroupID: "my-consumer-group",
        Topics:  []string{"my-events"},
    }

    // Create group consumer
    consumer, err := client.NewGroupConsumer(c, groupConfig)
    if err != nil {
        log.Fatal("Failed to create group consumer:", err)
    }
    defer consumer.Close()

    // Set up context for cancellation
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Handle shutdown signals
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-signals
        fmt.Println("\nShutting down...")
        cancel()
    }()

    // Start consuming with message handler
    fmt.Println("Group consumer started. Press Ctrl+C to stop...")

    err = consumer.Start(ctx, func(record *client.Record) error {
        fmt.Printf("📨 Group consumed: topic=%s, partition=%d, offset=%d\n",
            record.Topic, record.Partition, record.Offset)
        fmt.Printf("   Key: %s\n", string(record.Key))
        fmt.Printf("   Value: %s\n", string(record.Value))

        // Process the message here
        // Return error to retry, nil to commit
        return nil
    })

    if err != nil && err != context.Canceled {
        log.Fatal("Consumer error:", err)
    }
}
```

### Running Multiple Group Consumers

Open multiple terminals and run the same consumer:

```bash
# Terminal 1
go run group_consumer.go

# Terminal 2
go run group_consumer.go

# Terminal 3
go run group_consumer.go
```

StreamBus will automatically distribute partitions among the consumers in the group.

## Configuration Options

### Connection Configuration

```go
config := &client.Config{
    // Broker endpoints
    Brokers: []string{"broker1:9092", "broker2:9092"},

    // Timeouts
    ConnectTimeout: 10 * time.Second,
    RequestTimeout: 30 * time.Second,

    // Connection pooling
    MaxIdleConns:    10,
    MaxConnsPerHost: 100,
    IdleConnTimeout: 90 * time.Second,

    // Retry settings
    MaxRetries:   3,
    RetryBackoff: 100 * time.Millisecond,
}
```

### Secure Connections

#### TLS/mTLS

```go
config.Security = &client.SecurityConfig{
    TLS: &client.TLSConfig{
        Enabled:    true,
        CAFile:     "/path/to/ca.crt",
        CertFile:   "/path/to/client.crt",
        KeyFile:    "/path/to/client.key",
        ServerName: "streambus.example.com",
    },
}
```

#### SASL Authentication

```go
config.Security = &client.SecurityConfig{
    SASL: &client.SASLConfig{
        Enabled:   true,
        Mechanism: "SCRAM-SHA-256",
        Username:  "myuser",
        Password:  "mypassword",
    },
}
```

## Error Handling

Always handle errors appropriately in production:

```go
if err := producer.Send("topic", key, value); err != nil {
    switch {
    case errors.Is(err, client.ErrConnectionFailed):
        // Reconnect or circuit break
        log.Error("Connection failed, implementing backoff...")
        time.Sleep(time.Second * 5)

    case errors.Is(err, client.ErrTimeout):
        // Retry with backoff
        log.Warn("Request timed out, retrying...")

    default:
        // Log and handle unexpected errors
        log.Error("Unexpected error:", err)
    }
}
```

## Best Practices

### 1. Connection Management

- **Reuse connections**: Create one client and share it across producers/consumers
- **Handle disconnections**: Implement reconnection logic with exponential backoff
- **Close properly**: Always defer Close() calls

### 2. Producer Patterns

- **Batch messages**: Use transactions for better throughput
- **Handle failures**: Implement retry logic with dead letter queues
- **Monitor performance**: Track send latencies and success rates

### 3. Consumer Patterns

- **Commit strategies**: Choose between auto-commit and manual commits
- **Error handling**: Decide whether to retry, skip, or dead-letter failed messages
- **Scaling**: Use consumer groups for horizontal scaling

### 4. Monitoring

```go
// Add metrics collection
metrics := &client.Metrics{
    OnSend: func(topic string, latency time.Duration, err error) {
        // Record metrics
    },
    OnFetch: func(topic string, partition int, count int) {
        // Record metrics
    },
}
config.Metrics = metrics
```

## Next Steps

Congratulations! You've learned the basics of StreamBus SDK. Here's what to explore next:

- **[API Reference](./api-reference)** - Complete API documentation
- **[Architecture](./architecture)** - Understanding SDK internals
- **[Examples](./examples)** - More complex examples
- **[Best Practices](./best-practices)** - Production guidelines

## Need Help?

- 📚 Check the [API Reference](./api-reference)
- 💬 Ask in [GitHub Discussions](https://github.com/gstreamio/streambus-sdk/discussions)
- 🐛 Report issues on [GitHub](https://github.com/gstreamio/streambus-sdk/issues)