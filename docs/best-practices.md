---
layout: default
title: Best Practices - StreamBus SDK
---

# Best Practices

Production deployment guidelines and recommendations for using the StreamBus SDK effectively.

## Connection Management

### Single Client Instance

✅ **DO:** Create one client and share it

```go
// GOOD - Single shared client
type Application struct {
    client *client.Client
}

func NewApplication() (*Application, error) {
    config := client.DefaultConfig()
    config.Brokers = []string{"broker1:9092", "broker2:9092"}

    c, err := client.New(config)
    if err != nil {
        return nil, err
    }

    return &Application{client: c}, nil
}
```

❌ **DON'T:** Create multiple clients unnecessarily

```go
// BAD - Multiple client instances
func processMessage() {
    c, _ := client.New(config)  // Don't create per operation
    defer c.Close()
    // ...
}
```

### Connection Configuration

```go
config := &client.Config{
    // Use multiple brokers for HA
    Brokers: []string{
        "broker1.prod:9092",
        "broker2.prod:9092",
        "broker3.prod:9092",
    },

    // Tune timeouts for your network
    ConnectTimeout: 10 * time.Second,
    RequestTimeout: 30 * time.Second,

    // Optimize connection pooling
    MaxIdleConns:    20,  // Adjust based on load
    MaxConnsPerHost: 50,  // Prevent connection exhaustion
    IdleConnTimeout: 5 * time.Minute,

    // Configure retries
    MaxRetries:   3,
    RetryBackoff: time.Second,
}
```

### Graceful Shutdown

Always close resources properly:

```go
func main() {
    app, err := NewApplication()
    if err != nil {
        log.Fatal(err)
    }

    // Ensure cleanup
    defer func() {
        log.Println("Shutting down...")
        if err := app.Shutdown(); err != nil {
            log.Printf("Shutdown error: %v", err)
        }
    }()

    // Run application
    app.Run()
}
```

## Producer Best Practices

### Batching for Throughput

```go
// Use transactional producer for batching
txProducer, _ := client.NewTransactionalProducer(c, &client.TransactionalProducerConfig{
    TransactionID: "batch-producer-1",
})

// Batch multiple messages
txProducer.BeginTransaction()
for _, msg := range messages {
    if err := txProducer.Send(topic, msg.Key, msg.Value); err != nil {
        txProducer.AbortTransaction()
        return err
    }
}
txProducer.CommitTransaction()
```

### Async Production

For maximum throughput, use async sending:

```go
type AsyncProducer struct {
    producer *client.Producer
    pending  chan *Message
    wg       sync.WaitGroup
}

func (ap *AsyncProducer) Start() {
    for i := 0; i < 10; i++ {  // Multiple workers
        ap.wg.Add(1)
        go ap.worker()
    }
}

func (ap *AsyncProducer) worker() {
    defer ap.wg.Done()
    for msg := range ap.pending {
        if err := ap.producer.Send(msg.Topic, msg.Key, msg.Value); err != nil {
            log.Printf("Send failed: %v", err)
            // Handle error (retry, DLQ, etc.)
        }
    }
}
```

### Error Handling

```go
func sendWithErrorHandling(producer *client.Producer, topic string, key, value []byte) error {
    err := producer.Send(topic, key, value)
    if err != nil {
        // Classify error
        switch {
        case errors.Is(err, client.ErrMessageTooLarge):
            // Don't retry - message needs to be split
            return fmt.Errorf("message too large, cannot send")

        case errors.Is(err, client.ErrTopicNotFound):
            // Create topic or fail
            return fmt.Errorf("topic does not exist: %s", topic)

        case errors.Is(err, client.ErrTimeout):
            // Retry with backoff
            return retryWithBackoff(func() error {
                return producer.Send(topic, key, value)
            })

        default:
            // Unknown error - log and retry
            log.Printf("Unexpected error: %v", err)
            return err
        }
    }
    return nil
}
```

## Consumer Best Practices

### Offset Management

#### Manual Commit for Control

```go
consumer := client.NewConsumer(c, "events", 0)

// Process in batches with manual commit
batch := make([]*client.Record, 0, 100)
for {
    record, err := consumer.Fetch()
    if err != nil {
        continue
    }

    batch = append(batch, record)

    if len(batch) >= 100 {
        if err := processBatch(batch); err != nil {
            log.Printf("Batch processing failed: %v", err)
            // Don't commit - will reprocess
            continue
        }

        // Commit after successful processing
        if err := consumer.Commit(batch[len(batch)-1].Offset); err != nil {
            log.Printf("Commit failed: %v", err)
        }

        batch = batch[:0]  // Reset batch
    }
}
```

#### Auto-Commit for Simplicity

```go
groupConfig := &client.GroupConsumerConfig{
    GroupID:        "my-service",
    Topics:         []string{"events"},
    AutoCommit:     true,
    CommitInterval: 5 * time.Second,
}
```

### Consumer Group Patterns

#### Parallel Processing

```go
func processWithWorkerPool(consumer *client.GroupConsumer, workers int) error {
    sem := make(chan struct{}, workers)  // Limit concurrency

    return consumer.Start(context.Background(), func(record *client.Record) error {
        sem <- struct{}{}  // Acquire
        go func() {
            defer func() { <-sem }()  // Release

            if err := processRecord(record); err != nil {
                log.Printf("Processing failed: %v", err)
            }
        }()

        return nil  // Commit immediately
    })
}
```

#### Sequential Processing

```go
func processSequentially(consumer *client.GroupConsumer) error {
    return consumer.Start(context.Background(), func(record *client.Record) error {
        // Process one at a time
        if err := processRecord(record); err != nil {
            // Return error to retry
            return err
        }

        // Return nil to commit
        return nil
    })
}
```

### Poison Pill Handling

```go
func handlePoisonPills(record *client.Record) error {
    retries := 0
    maxRetries := 3

    for retries < maxRetries {
        err := processRecord(record)
        if err == nil {
            return nil
        }

        if isPermanentError(err) {
            // Send to DLQ
            sendToDLQ(record, err)
            return nil  // Continue processing
        }

        retries++
        time.Sleep(time.Duration(retries) * time.Second)
    }

    // Max retries exceeded - send to DLQ
    sendToDLQ(record, fmt.Errorf("max retries exceeded"))
    return nil
}
```

## Security Best Practices

### TLS Configuration

```go
config.Security = &client.SecurityConfig{
    TLS: &client.TLSConfig{
        Enabled:    true,
        CAFile:     "/secure/ca.crt",
        CertFile:   "/secure/client.crt",
        KeyFile:    "/secure/client.key",
        ServerName: "streambus.prod.example.com",

        // Never skip verification in production
        SkipVerify: false,
    },
}
```

### Credential Management

```go
// Use environment variables or secret management
config.Security = &client.SecurityConfig{
    SASL: &client.SASLConfig{
        Enabled:   true,
        Mechanism: "SCRAM-SHA-256",
        Username:  os.Getenv("STREAMBUS_USERNAME"),
        Password:  os.Getenv("STREAMBUS_PASSWORD"),
    },
}
```

### Key Rotation

```go
type RotatingCredentials struct {
    mu       sync.RWMutex
    username string
    password string
}

func (rc *RotatingCredentials) GetCredentials() (string, string) {
    rc.mu.RLock()
    defer rc.mu.RUnlock()
    return rc.username, rc.password
}

func (rc *RotatingCredentials) UpdateCredentials(username, password string) {
    rc.mu.Lock()
    defer rc.mu.Unlock()
    rc.username = username
    rc.password = password
}
```

## Monitoring & Observability

### Metrics Collection

```go
import (
    "github.com/prometheus/client_golang/prometheus"
)

var (
    messagesProcessed = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "streambus_messages_processed_total",
            Help: "Total messages processed",
        },
        []string{"topic", "status"},
    )

    processingDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "streambus_processing_duration_seconds",
            Help:    "Message processing duration",
            Buckets: prometheus.DefBuckets,
        },
        []string{"topic"},
    )
)

func instrumentedHandler(record *client.Record) error {
    timer := prometheus.NewTimer(
        processingDuration.WithLabelValues(record.Topic),
    )
    defer timer.ObserveDuration()

    err := processRecord(record)

    if err != nil {
        messagesProcessed.WithLabelValues(record.Topic, "error").Inc()
        return err
    }

    messagesProcessed.WithLabelValues(record.Topic, "success").Inc()
    return nil
}
```

### Logging

```go
import (
    "github.com/sirupsen/logrus"
)

func setupLogging() {
    log := logrus.New()
    log.SetFormatter(&logrus.JSONFormatter{})
    log.SetLevel(logrus.InfoLevel)

    // Add context to logs
    log.WithFields(logrus.Fields{
        "service": "streambus-consumer",
        "version": "1.0.0",
    }).Info("Service started")
}
```

### Health Checks

```go
func healthCheck(c *client.Client) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // Check broker connectivity
        if _, err := c.ListTopics(); err != nil {
            w.WriteHeader(http.StatusServiceUnavailable)
            json.NewEncoder(w).Encode(map[string]string{
                "status": "unhealthy",
                "error":  err.Error(),
            })
            return
        }

        w.WriteHeader(http.StatusOK)
        json.NewEncoder(w).Encode(map[string]string{
            "status": "healthy",
        })
    }
}
```

## Performance Tuning

### OS Tuning

```bash
# Increase file descriptors
ulimit -n 65536

# TCP tuning
sysctl -w net.core.rmem_max=134217728
sysctl -w net.core.wmem_max=134217728
sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"
```

### Go Runtime Tuning

```go
import "runtime"

func init() {
    // Set GOMAXPROCS for containerized environments
    runtime.GOMAXPROCS(runtime.NumCPU())

    // Tune GC for lower latency
    runtime.SetGCPercent(100)
}
```

### Connection Pool Tuning

```go
config := &client.Config{
    // High-throughput settings
    MaxIdleConns:    50,
    MaxConnsPerHost: 200,
    IdleConnTimeout: 10 * time.Minute,

    // Fast failure detection
    ConnectTimeout: 5 * time.Second,
    RequestTimeout: 10 * time.Second,
}
```

## Testing Strategies

### Unit Testing

```go
func TestMessageProcessor(t *testing.T) {
    // Use mock producer
    mockProducer := &MockProducer{}
    processor := NewProcessor(mockProducer)

    err := processor.Process(testMessage)
    assert.NoError(t, err)
    assert.Len(t, mockProducer.Messages, 1)
}
```

### Integration Testing

```go
func TestIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test")
    }

    // Use test containers or embedded broker
    broker := startTestBroker(t)
    defer broker.Stop()

    config := client.DefaultConfig()
    config.Brokers = []string{broker.Address()}

    c, err := client.New(config)
    require.NoError(t, err)
    defer c.Close()

    // Run integration tests
}
```

### Load Testing

```go
func BenchmarkProducer(b *testing.B) {
    producer := setupProducer(b)
    defer producer.Close()

    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            err := producer.Send("bench", nil, testPayload)
            if err != nil {
                b.Fatal(err)
            }
        }
    })
}
```

## Deployment Considerations

### Container Deployments

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o streambus-app

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/streambus-app /streambus-app
CMD ["/streambus-app"]
```

### Kubernetes Configuration

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: streambus-consumer
spec:
  replicas: 3  # Match partition count
  template:
    spec:
      containers:
      - name: consumer
        image: myapp:latest
        env:
        - name: STREAMBUS_BROKERS
          value: "broker-0:9092,broker-1:9092,broker-2:9092"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
```

### Environment Configuration

```bash
# Production settings
export STREAMBUS_BROKERS="broker1:9092,broker2:9092,broker3:9092"
export STREAMBUS_TLS_ENABLED="true"
export STREAMBUS_TLS_CA_FILE="/certs/ca.crt"
export STREAMBUS_SASL_USERNAME="${SECRET_USERNAME}"
export STREAMBUS_SASL_PASSWORD="${SECRET_PASSWORD}"
export STREAMBUS_MAX_RETRIES="5"
export STREAMBUS_BATCH_SIZE="1000"
```

## Common Pitfalls to Avoid

1. **Creating multiple clients** - Reuse a single client instance
2. **Ignoring errors** - Always handle and log errors appropriately
3. **Not committing offsets** - Ensure proper offset management
4. **Blocking in handlers** - Use goroutines for long-running operations
5. **Hardcoding configuration** - Use environment variables
6. **Skipping health checks** - Implement proper health endpoints
7. **No monitoring** - Add metrics and logging from day one
8. **No graceful shutdown** - Handle signals and cleanup resources
9. **Single point of failure** - Use multiple brokers and replicas
10. **No testing** - Write comprehensive tests including integration tests

## Summary

Following these best practices will help you build robust, scalable, and maintainable streaming applications with the StreamBus SDK. Remember to:

- Start simple and iterate
- Monitor everything
- Handle errors gracefully
- Test thoroughly
- Document your decisions

For more information, see our [Examples](./examples) and [API Reference](./api-reference).