---
layout: default
title: Examples - StreamBus SDK
---

# Examples

Real-world examples and common patterns for using the StreamBus Go SDK.

## Basic Examples

### Simple Producer

Send messages to a StreamBus topic:

```go
package main

import (
    "fmt"
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

    producer := client.NewProducer(c)
    defer producer.Close()

    // Send a single message
    err = producer.Send("events",
        []byte("user-123"),
        []byte(`{"action": "login", "timestamp": "2024-01-15T10:30:00Z"}`))

    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Message sent successfully!")
}
```

### Simple Consumer

Consume messages from a specific partition:

```go
package main

import (
    "fmt"
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

    consumer := client.NewConsumer(c, "events", 0)
    defer consumer.Close()

    // Start from beginning
    consumer.Seek(0)

    for i := 0; i < 10; i++ {
        record, err := consumer.Fetch()
        if err != nil {
            log.Printf("Error: %v", err)
            continue
        }

        fmt.Printf("Message: %s\n", string(record.Value))
    }
}
```

## Advanced Patterns

### Retry Logic with Exponential Backoff

```go
package main

import (
    "fmt"
    "log"
    "math"
    "time"
    "github.com/gstreamio/streambus-sdk/client"
)

func sendWithRetry(producer *client.Producer, topic string, key, value []byte) error {
    maxRetries := 5
    baseDelay := 100 * time.Millisecond

    for attempt := 0; attempt < maxRetries; attempt++ {
        err := producer.Send(topic, key, value)
        if err == nil {
            return nil
        }

        if attempt == maxRetries-1 {
            return fmt.Errorf("failed after %d attempts: %w", maxRetries, err)
        }

        // Exponential backoff with jitter
        delay := time.Duration(math.Pow(2, float64(attempt))) * baseDelay
        jitter := time.Duration(rand.Int63n(int64(delay / 2)))
        time.Sleep(delay + jitter)

        log.Printf("Retry %d/%d after %v", attempt+1, maxRetries, delay)
    }

    return nil
}
```

### Dead Letter Queue Pattern

```go
package main

import (
    "encoding/json"
    "log"
    "github.com/gstreamio/streambus-sdk/client"
)

type MessageProcessor struct {
    client   *client.Client
    producer *client.Producer
    dlqTopic string
}

func (mp *MessageProcessor) ProcessWithDLQ(record *client.Record) error {
    // Try to process the message
    err := mp.processMessage(record)
    if err != nil {
        // Send to DLQ on failure
        dlqErr := mp.sendToDLQ(record, err)
        if dlqErr != nil {
            log.Printf("Failed to send to DLQ: %v", dlqErr)
        }
        return err
    }
    return nil
}

func (mp *MessageProcessor) sendToDLQ(record *client.Record, originalError error) error {
    // Wrap the failed message with metadata
    dlqMessage := map[string]interface{}{
        "original_topic":     record.Topic,
        "original_partition": record.Partition,
        "original_offset":    record.Offset,
        "original_key":       string(record.Key),
        "original_value":     string(record.Value),
        "error":             originalError.Error(),
        "timestamp":         record.Timestamp,
    }

    value, err := json.Marshal(dlqMessage)
    if err != nil {
        return err
    }

    return mp.producer.Send(mp.dlqTopic, record.Key, value)
}

func (mp *MessageProcessor) processMessage(record *client.Record) error {
    // Your processing logic here
    return nil
}
```

### Circuit Breaker Pattern

```go
package main

import (
    "errors"
    "sync"
    "time"
    "github.com/gstreamio/streambus-sdk/client"
)

type CircuitBreaker struct {
    producer        *client.Producer
    failureThreshold int
    resetTimeout    time.Duration

    mu           sync.Mutex
    failures     int
    lastFailTime time.Time
    state        string // "closed", "open", "half-open"
}

func NewCircuitBreaker(producer *client.Producer) *CircuitBreaker {
    return &CircuitBreaker{
        producer:         producer,
        failureThreshold: 5,
        resetTimeout:     30 * time.Second,
        state:           "closed",
    }
}

func (cb *CircuitBreaker) Send(topic string, key, value []byte) error {
    cb.mu.Lock()
    defer cb.mu.Unlock()

    // Check circuit state
    if cb.state == "open" {
        if time.Since(cb.lastFailTime) > cb.resetTimeout {
            cb.state = "half-open"
            cb.failures = 0
        } else {
            return errors.New("circuit breaker is open")
        }
    }

    // Try to send
    err := cb.producer.Send(topic, key, value)

    if err != nil {
        cb.failures++
        cb.lastFailTime = time.Now()

        if cb.failures >= cb.failureThreshold {
            cb.state = "open"
            return errors.New("circuit breaker opened due to failures")
        }
        return err
    }

    // Success - reset if in half-open state
    if cb.state == "half-open" {
        cb.state = "closed"
        cb.failures = 0
    }

    return nil
}
```

## Production Patterns

### Health Check Endpoint

```go
package main

import (
    "encoding/json"
    "net/http"
    "github.com/gstreamio/streambus-sdk/client"
)

type HealthChecker struct {
    client *client.Client
}

func (hc *HealthChecker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    status := map[string]interface{}{
        "status": "healthy",
        "checks": map[string]string{},
    }

    // Check broker connectivity
    topics, err := hc.client.ListTopics()
    if err != nil {
        status["status"] = "unhealthy"
        status["checks"].(map[string]string)["brokers"] = "failed: " + err.Error()
    } else {
        status["checks"].(map[string]string)["brokers"] = "ok"
        status["checks"].(map[string]string)["topics_count"] = fmt.Sprintf("%d", len(topics))
    }

    // Set appropriate status code
    if status["status"] == "unhealthy" {
        w.WriteHeader(http.StatusServiceUnavailable)
    }

    json.NewEncoder(w).Encode(status)
}

func main() {
    config := client.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    c, err := client.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    health := &HealthChecker{client: c}
    http.Handle("/health", health)

    log.Println("Health check available at http://localhost:8080/health")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

### Graceful Shutdown

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "time"
    "github.com/gstreamio/streambus-sdk/client"
)

type Application struct {
    client   *client.Client
    producer *client.Producer
    consumer *client.GroupConsumer
    wg       sync.WaitGroup
    shutdown chan struct{}
}

func (app *Application) Start() error {
    // Start consumer in background
    app.wg.Add(1)
    go app.consumeMessages()

    // Start producer in background
    app.wg.Add(1)
    go app.produceMessages()

    // Wait for shutdown signal
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan

    log.Println("Shutting down gracefully...")
    return app.Shutdown()
}

func (app *Application) Shutdown() error {
    // Signal shutdown
    close(app.shutdown)

    // Wait for goroutines with timeout
    done := make(chan struct{})
    go func() {
        app.wg.Wait()
        close(done)
    }()

    select {
    case <-done:
        log.Println("All workers stopped")
    case <-time.After(30 * time.Second):
        log.Println("Shutdown timeout exceeded")
    }

    // Close resources
    if app.consumer != nil {
        app.consumer.Close()
    }
    if app.producer != nil {
        app.producer.Close()
    }
    if app.client != nil {
        app.client.Close()
    }

    return nil
}

func (app *Application) consumeMessages() {
    defer app.wg.Done()

    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        <-app.shutdown
        cancel()
    }()

    err := app.consumer.Start(ctx, func(record *client.Record) error {
        // Process message
        log.Printf("Processing: %s", string(record.Value))
        return nil
    })

    if err != nil && err != context.Canceled {
        log.Printf("Consumer error: %v", err)
    }
}

func (app *Application) produceMessages() {
    defer app.wg.Done()

    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-app.shutdown:
            return
        case <-ticker.C:
            msg := fmt.Sprintf("Message at %s", time.Now())
            if err := app.producer.Send("events", nil, []byte(msg)); err != nil {
                log.Printf("Send error: %v", err)
            }
        }
    }
}
```

### Metrics Collection

```go
package main

import (
    "log"
    "time"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
    "github.com/gstreamio/streambus-sdk/client"
)

var (
    messagesSent = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "streambus_messages_sent_total",
        Help: "Total number of messages sent",
    }, []string{"topic", "status"})

    messagesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "streambus_messages_received_total",
        Help: "Total number of messages received",
    }, []string{"topic"})

    sendLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "streambus_send_latency_seconds",
        Help:    "Send latency in seconds",
        Buckets: prometheus.DefBuckets,
    }, []string{"topic"})
)

type MetricsProducer struct {
    producer *client.Producer
}

func (mp *MetricsProducer) Send(topic string, key, value []byte) error {
    start := time.Now()

    err := mp.producer.Send(topic, key, value)

    duration := time.Since(start).Seconds()
    sendLatency.WithLabelValues(topic).Observe(duration)

    if err != nil {
        messagesSent.WithLabelValues(topic, "error").Inc()
        return err
    }

    messagesSent.WithLabelValues(topic, "success").Inc()
    return nil
}

type MetricsHandler struct{}

func (mh *MetricsHandler) Handle(record *client.Record) error {
    messagesReceived.WithLabelValues(record.Topic).Inc()

    // Process the message
    return processMessage(record)
}

func processMessage(record *client.Record) error {
    // Your processing logic
    return nil
}
```

## Testing Examples

### Mock Client for Testing

```go
package main

import (
    "testing"
    "github.com/gstreamio/streambus-sdk/client"
)

type MockProducer struct {
    Messages []Message
    Error    error
}

type Message struct {
    Topic string
    Key   []byte
    Value []byte
}

func (mp *MockProducer) Send(topic string, key, value []byte) error {
    if mp.Error != nil {
        return mp.Error
    }

    mp.Messages = append(mp.Messages, Message{
        Topic: topic,
        Key:   key,
        Value: value,
    })

    return nil
}

func TestMyService(t *testing.T) {
    mockProducer := &MockProducer{}

    // Test your service with mock
    service := NewMyService(mockProducer)
    err := service.ProcessEvent("test-event")

    if err != nil {
        t.Fatalf("ProcessEvent failed: %v", err)
    }

    // Verify messages were sent
    if len(mockProducer.Messages) != 1 {
        t.Fatalf("Expected 1 message, got %d", len(mockProducer.Messages))
    }

    msg := mockProducer.Messages[0]
    if msg.Topic != "events" {
        t.Errorf("Expected topic 'events', got %s", msg.Topic)
    }
}
```

## Integration Examples

### HTTP to StreamBus Bridge

```go
package main

import (
    "encoding/json"
    "io"
    "net/http"
    "github.com/gstreamio/streambus-sdk/client"
)

type HTTPBridge struct {
    producer *client.Producer
}

func (hb *HTTPBridge) HandleWebhook(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    body, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "Failed to read body", http.StatusBadRequest)
        return
    }

    // Get topic from header or use default
    topic := r.Header.Get("X-StreamBus-Topic")
    if topic == "" {
        topic = "webhooks"
    }

    // Use request ID as key if available
    key := []byte(r.Header.Get("X-Request-ID"))

    // Send to StreamBus
    if err := hb.producer.Send(topic, key, body); err != nil {
        http.Error(w, "Failed to send message", http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusAccepted)
    json.NewEncoder(w).Encode(map[string]string{
        "status": "accepted",
        "topic":  topic,
    })
}

func main() {
    config := client.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    c, err := client.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    producer := client.NewProducer(c)
    defer producer.Close()

    bridge := &HTTPBridge{producer: producer}
    http.HandleFunc("/webhook", bridge.HandleWebhook)

    log.Println("HTTP to StreamBus bridge listening on :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

## Next Steps

- Review the [API Reference](./api-reference) for complete documentation
- Learn about [Best Practices](./best-practices) for production deployments
- Explore the [Architecture](./architecture) to understand SDK internals