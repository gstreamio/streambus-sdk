---
layout: default
title: StreamBus Go SDK Documentation
---

# StreamBus Go SDK

<div class="hero-section">
  <h2>Build High-Performance Streaming Applications</h2>
  <p>A modern, lightweight Go client library for StreamBus with enterprise-grade features</p>
  <div class="hero-buttons">
    <a href="./getting-started" class="btn btn-primary">Get Started</a>
    <a href="https://github.com/gstreamio/streambus-sdk" class="btn btn-secondary">View on GitHub</a>
  </div>
</div>

## Why StreamBus SDK?

StreamBus SDK is designed to make building distributed streaming applications in Go simple, efficient, and reliable.

<div class="features-grid">
  <div class="feature-card">
    <h3>🚀 High Performance</h3>
    <p>Achieve 1M+ messages/sec with minimal latency and resource usage</p>
  </div>
  <div class="feature-card">
    <h3>🔒 Enterprise Security</h3>
    <p>Built-in support for TLS/mTLS and SASL authentication</p>
  </div>
  <div class="feature-card">
    <h3>⚡ Zero Dependencies</h3>
    <p>Minimal external dependencies for maximum reliability</p>
  </div>
  <div class="feature-card">
    <h3>🎯 Simple API</h3>
    <p>Intuitive Go-idiomatic API that's easy to learn and use</p>
  </div>
</div>

## Quick Example

```go
package main

import (
    "log"
    "github.com/gstreamio/streambus-sdk/client"
)

func main() {
    // Connect to StreamBus
    config := client.DefaultConfig()
    config.Brokers = []string{"localhost:9092"}

    c, err := client.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Send a message
    producer := client.NewProducer(c)
    err = producer.Send("events", []byte("key"), []byte("Hello StreamBus!"))
    if err != nil {
        log.Fatal(err)
    }

    log.Println("Message sent successfully!")
}
```

## Core Features

### Message Production
- **Synchronous sending** with delivery confirmation
- **Asynchronous sending** for maximum throughput
- **Transactional support** for exactly-once semantics
- **Automatic retries** with configurable backoff

### Message Consumption
- **Simple consumers** for direct partition access
- **Consumer groups** with automatic rebalancing
- **Offset management** with commit strategies
- **Transactional reads** for consistency

### Advanced Capabilities
- **Connection pooling** for efficient resource usage
- **Circuit breakers** for fault tolerance
- **Metrics collection** for monitoring
- **Comprehensive logging** for debugging

## Getting Started

Ready to build your first StreamBus application?

<div class="cta-section">
  <a href="./getting-started" class="btn btn-large">Start Tutorial →</a>
</div>

## Documentation

<div class="docs-links">
  <div class="doc-link">
    <h4><a href="./getting-started">Getting Started Guide</a></h4>
    <p>Step-by-step tutorial to get up and running quickly</p>
  </div>
  <div class="doc-link">
    <h4><a href="./api-reference">API Reference</a></h4>
    <p>Complete reference for all SDK types and functions</p>
  </div>
  <div class="doc-link">
    <h4><a href="./architecture">Architecture Overview</a></h4>
    <p>Understanding the SDK design and internals</p>
  </div>
  <div class="doc-link">
    <h4><a href="./examples">Examples</a></h4>
    <p>Real-world examples and common patterns</p>
  </div>
  <div class="doc-link">
    <h4><a href="./best-practices">Best Practices</a></h4>
    <p>Production deployment guidelines and tips</p>
  </div>
</div>

## Community & Support

- 🐛 [Report Issues](https://github.com/gstreamio/streambus-sdk/issues)
- 💬 [Join Discussions](https://github.com/gstreamio/streambus-sdk/discussions)
- ⭐ [Star on GitHub](https://github.com/gstreamio/streambus-sdk)

## License

StreamBus SDK is licensed under the [Apache License 2.0](https://github.com/gstreamio/streambus-sdk/blob/main/LICENSE).