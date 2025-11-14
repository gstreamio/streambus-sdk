---
layout: default
title: API Reference - StreamBus SDK
---

# API Reference

Complete reference documentation for the StreamBus Go SDK.

## Package: `client`

```go
import "github.com/gstreamio/streambus-sdk/client"
```

## Client

The main client for connecting to StreamBus brokers.

### Types

#### `Client`

```go
type Client struct {
    // contains filtered or unexported fields
}
```

The Client manages connections to StreamBus brokers and provides methods for topic management.

#### `Config`

```go
type Config struct {
    // Broker addresses
    Brokers []string

    // Connection settings
    ConnectTimeout time.Duration
    RequestTimeout time.Duration

    // Connection pooling
    MaxIdleConns    int
    MaxConnsPerHost int
    IdleConnTimeout time.Duration

    // Retry configuration
    MaxRetries   int
    RetryBackoff time.Duration

    // Security settings
    Security *SecurityConfig

    // Metrics collection
    Metrics *Metrics
}
```

### Functions

#### `DefaultConfig()`

```go
func DefaultConfig() *Config
```

Returns a Config with sensible defaults:
- ConnectTimeout: 10 seconds
- RequestTimeout: 30 seconds
- MaxRetries: 3
- RetryBackoff: 100ms

#### `New()`

```go
func New(config *Config) (*Client, error)
```

Creates a new Client with the given configuration.

**Example:**
```go
config := client.DefaultConfig()
config.Brokers = []string{"localhost:9092"}

c, err := client.New(config)
if err != nil {
    log.Fatal(err)
}
defer c.Close()
```

### Methods

#### `CreateTopic()`

```go
func (c *Client) CreateTopic(name string, partitions, replicas int) error
```

Creates a new topic with the specified number of partitions and replicas.

**Parameters:**
- `name`: Topic name (must be unique)
- `partitions`: Number of partitions (1-1000)
- `replicas`: Replication factor (1-10)

**Returns:**
- `error`: ErrTopicExists if topic already exists

#### `DeleteTopic()`

```go
func (c *Client) DeleteTopic(name string) error
```

Deletes a topic and all its data.

**Parameters:**
- `name`: Topic name to delete

**Returns:**
- `error`: ErrTopicNotFound if topic doesn't exist

#### `ListTopics()`

```go
func (c *Client) ListTopics() ([]string, error)
```

Returns a list of all topic names.

#### `Close()`

```go
func (c *Client) Close() error
```

Closes the client and all associated connections.

---

## Producer

### Types

#### `Producer`

```go
type Producer struct {
    // contains filtered or unexported fields
}
```

A Producer sends messages to StreamBus topics.

#### `ProducerConfig`

```go
type ProducerConfig struct {
    // Batching settings
    BatchSize    int
    BatchTimeout time.Duration

    // Compression
    Compression CompressionType

    // Acknowledgment level
    RequiredAcks int
}
```

### Functions

#### `NewProducer()`

```go
func NewProducer(client *Client) *Producer
```

Creates a new Producer with default settings.

#### `NewProducerWithConfig()`

```go
func NewProducerWithConfig(client *Client, config *ProducerConfig) *Producer
```

Creates a new Producer with custom configuration.

### Methods

#### `Send()`

```go
func (p *Producer) Send(topic string, key, value []byte) error
```

Sends a message to the specified topic.

**Parameters:**
- `topic`: Target topic name
- `key`: Message key (can be nil)
- `value`: Message value

**Returns:**
- `error`: nil on success

**Example:**
```go
err := producer.Send("events", []byte("key"), []byte("value"))
if err != nil {
    log.Printf("Send failed: %v", err)
}
```

#### `SendAsync()`

```go
func (p *Producer) SendAsync(topic string, key, value []byte, callback func(error))
```

Sends a message asynchronously with a callback.

**Parameters:**
- `topic`: Target topic name
- `key`: Message key (can be nil)
- `value`: Message value
- `callback`: Function called when send completes

#### `Close()`

```go
func (p *Producer) Close() error
```

Flushes pending messages and closes the producer.

---

## Consumer

### Types

#### `Consumer`

```go
type Consumer struct {
    // contains filtered or unexported fields
}
```

A Consumer reads messages from a specific topic partition.

#### `Record`

```go
type Record struct {
    Topic     string
    Partition int
    Offset    int64
    Key       []byte
    Value     []byte
    Timestamp time.Time
    Headers   map[string]string
}
```

Represents a message consumed from StreamBus.

### Functions

#### `NewConsumer()`

```go
func NewConsumer(client *Client, topic string, partition int) *Consumer
```

Creates a consumer for a specific partition.

**Parameters:**
- `client`: StreamBus client
- `topic`: Topic to consume from
- `partition`: Partition number (0-based)

### Methods

#### `Seek()`

```go
func (c *Consumer) Seek(offset int64) error
```

Sets the consumer's position in the partition.

**Parameters:**
- `offset`: Absolute offset position
  - Use 0 for beginning
  - Use -1 for end
  - Use -2 for latest committed

#### `Fetch()`

```go
func (c *Consumer) Fetch() (*Record, error)
```

Fetches the next message from the partition.

**Returns:**
- `*Record`: The next message
- `error`: ErrNoMessages if no messages available

**Example:**
```go
for {
    record, err := consumer.Fetch()
    if err != nil {
        if errors.Is(err, client.ErrNoMessages) {
            time.Sleep(time.Second)
            continue
        }
        log.Fatal(err)
    }

    processMessage(record)
}
```

#### `FetchBatch()`

```go
func (c *Consumer) FetchBatch(maxMessages int) ([]*Record, error)
```

Fetches multiple messages at once.

**Parameters:**
- `maxMessages`: Maximum messages to fetch (1-1000)

#### `Commit()`

```go
func (c *Consumer) Commit(offset int64) error
```

Commits the consumer's position for recovery.

#### `Close()`

```go
func (c *Consumer) Close() error
```

Closes the consumer and releases resources.

---

## Group Consumer

### Types

#### `GroupConsumer`

```go
type GroupConsumer struct {
    // contains filtered or unexported fields
}
```

A GroupConsumer participates in a consumer group for automatic partition assignment.

#### `GroupConsumerConfig`

```go
type GroupConsumerConfig struct {
    GroupID string
    Topics  []string

    // Session settings
    SessionTimeout   time.Duration
    HeartbeatInterval time.Duration

    // Offset management
    AutoCommit       bool
    CommitInterval   time.Duration
    StartFromLatest  bool
}
```

#### `MessageHandler`

```go
type MessageHandler func(record *Record) error
```

Function type for processing messages. Return nil to acknowledge, error to retry.

### Functions

#### `NewGroupConsumer()`

```go
func NewGroupConsumer(client *Client, config *GroupConsumerConfig) (*GroupConsumer, error)
```

Creates a new consumer group member.

### Methods

#### `Start()`

```go
func (gc *GroupConsumer) Start(ctx context.Context, handler MessageHandler) error
```

Starts consuming messages with the provided handler.

**Parameters:**
- `ctx`: Context for cancellation
- `handler`: Function to process each message

**Example:**
```go
err := consumer.Start(ctx, func(record *Record) error {
    fmt.Printf("Processing: %s\n", string(record.Value))

    // Return nil to commit, error to retry
    return nil
})
```

#### `Close()`

```go
func (gc *GroupConsumer) Close() error
```

Leaves the consumer group and closes the consumer.

---

## Transactional Producer

### Types

#### `TransactionalProducer`

```go
type TransactionalProducer struct {
    // contains filtered or unexported fields
}
```

#### `TransactionalProducerConfig`

```go
type TransactionalProducerConfig struct {
    TransactionID string
    Timeout       time.Duration
}
```

### Functions

#### `NewTransactionalProducer()`

```go
func NewTransactionalProducer(client *Client, config *TransactionalProducerConfig) (*TransactionalProducer, error)
```

### Methods

#### `BeginTransaction()`

```go
func (tp *TransactionalProducer) BeginTransaction() error
```

Starts a new transaction.

#### `Send()`

```go
func (tp *TransactionalProducer) Send(topic string, key, value []byte) error
```

Sends a message within the current transaction.

#### `CommitTransaction()`

```go
func (tp *TransactionalProducer) CommitTransaction() error
```

Commits all messages in the current transaction.

#### `AbortTransaction()`

```go
func (tp *TransactionalProducer) AbortTransaction() error
```

Aborts the current transaction, discarding all messages.

---

## Security Configuration

### Types

#### `SecurityConfig`

```go
type SecurityConfig struct {
    TLS  *TLSConfig
    SASL *SASLConfig
}
```

#### `TLSConfig`

```go
type TLSConfig struct {
    Enabled    bool
    CAFile     string  // CA certificate file
    CertFile   string  // Client certificate file
    KeyFile    string  // Client private key file
    ServerName string  // Expected server name
    SkipVerify bool    // Skip certificate verification (insecure)
}
```

#### `SASLConfig`

```go
type SASLConfig struct {
    Enabled   bool
    Mechanism string  // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
    Username  string
    Password  string
}
```

---

## Error Types

### Common Errors

```go
var (
    // Connection errors
    ErrConnectionFailed = errors.New("connection failed")
    ErrConnectionClosed = errors.New("connection closed")
    ErrTimeout         = errors.New("operation timed out")

    // Topic errors
    ErrTopicNotFound = errors.New("topic not found")
    ErrTopicExists   = errors.New("topic already exists")
    ErrInvalidTopic  = errors.New("invalid topic name")

    // Consumer errors
    ErrNoMessages        = errors.New("no messages available")
    ErrOffsetOutOfRange  = errors.New("offset out of range")
    ErrConsumerClosed    = errors.New("consumer closed")

    // Producer errors
    ErrProducerClosed = errors.New("producer closed")
    ErrMessageTooLarge = errors.New("message too large")

    // Transaction errors
    ErrTransactionNotStarted = errors.New("transaction not started")
    ErrTransactionInProgress = errors.New("transaction already in progress")
)
```

### Error Checking

```go
if err != nil {
    switch {
    case errors.Is(err, client.ErrConnectionFailed):
        // Handle connection failure
    case errors.Is(err, client.ErrTimeout):
        // Handle timeout
    default:
        // Handle other errors
    }
}
```

---

## Constants

### Compression Types

```go
const (
    CompressionNone   CompressionType = 0
    CompressionGzip   CompressionType = 1
    CompressionSnappy CompressionType = 2
    CompressionLZ4    CompressionType = 3
    CompressionZstd   CompressionType = 4
)
```

### Acknowledgment Levels

```go
const (
    AckNone   = 0  // No acknowledgment
    AckLeader = 1  // Leader acknowledgment
    AckAll    = -1 // All replicas acknowledgment
)
```

### Isolation Levels

```go
const (
    ReadUncommitted IsolationLevel = 0
    ReadCommitted   IsolationLevel = 1
)
```

---

## Examples

### Complete Producer Example

```go
package main

import (
    "log"
    "github.com/gstreamio/streambus-sdk/client"
)

func main() {
    // Configure client
    config := client.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    // Add security
    config.Security = &client.SecurityConfig{
        TLS: &client.TLSConfig{
            Enabled: true,
            CAFile:  "/path/to/ca.crt",
        },
    }

    // Create client
    c, err := client.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Create producer with custom config
    producerConfig := &client.ProducerConfig{
        BatchSize:    100,
        Compression:  client.CompressionSnappy,
        RequiredAcks: client.AckAll,
    }

    producer := client.NewProducerWithConfig(c, producerConfig)
    defer producer.Close()

    // Send messages
    for i := 0; i < 1000; i++ {
        key := []byte(fmt.Sprintf("key-%d", i))
        value := []byte(fmt.Sprintf("message-%d", i))

        if err := producer.Send("events", key, value); err != nil {
            log.Printf("Send failed: %v", err)
        }
    }
}
```

### Complete Consumer Group Example

```go
package main

import (
    "context"
    "log"
    "github.com/gstreamio/streambus-sdk/client"
)

func main() {
    config := client.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    c, err := client.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Configure consumer group
    groupConfig := &client.GroupConsumerConfig{
        GroupID:          "my-service",
        Topics:           []string{"events", "commands"},
        SessionTimeout:   10 * time.Second,
        AutoCommit:       true,
        CommitInterval:   5 * time.Second,
        StartFromLatest:  false,
    }

    consumer, err := client.NewGroupConsumer(c, groupConfig)
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    // Process messages
    ctx := context.Background()
    err = consumer.Start(ctx, func(record *client.Record) error {
        // Process the message
        log.Printf("Processing message from %s", record.Topic)

        // Simulate processing
        if err := processMessage(record); err != nil {
            // Return error to retry
            return err
        }

        // Return nil to commit
        return nil
    })

    if err != nil {
        log.Fatal(err)
    }
}

func processMessage(record *client.Record) error {
    // Your message processing logic here
    return nil
}
```