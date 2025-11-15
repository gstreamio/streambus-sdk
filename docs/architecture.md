---
layout: default
title: Architecture - StreamBus SDK
---

# Architecture Overview

Understanding the internal architecture of the StreamBus Go SDK.

## Design Principles

The StreamBus SDK is built on several core principles:

1. **Simplicity** - Clean, idiomatic Go API that's easy to use
2. **Performance** - Minimal overhead and efficient resource usage
3. **Reliability** - Robust error handling and automatic recovery
4. **Flexibility** - Configurable for different use cases
5. **Zero Dependencies** - Minimal external dependencies

## Component Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Application                        │
└────────────┬────────────────────────┬────────────────┘
             │                        │
    ┌────────▼──────┐        ┌───────▼──────┐
    │   Producer    │        │   Consumer    │
    └────────┬──────┘        └───────┬──────┘
             │                        │
    ┌────────▼────────────────────────▼────────────┐
    │              Client Core                     │
    │  ┌─────────────────────────────────────┐    │
    │  │      Connection Pool Manager         │    │
    │  └─────────────────────────────────────┘    │
    │  ┌─────────────────────────────────────┐    │
    │  │       Protocol Handler               │    │
    │  └─────────────────────────────────────┘    │
    │  ┌─────────────────────────────────────┐    │
    │  │        Buffer Pool                   │    │
    │  └─────────────────────────────────────┘    │
    └──────────────────────┬───────────────────────┘
                           │
    ┌──────────────────────▼───────────────────────┐
    │            StreamBus Brokers                  │
    └───────────────────────────────────────────────┘
```

## Core Components

### Client

The Client is the central component that manages:
- Broker discovery and metadata
- Connection lifecycle
- Topic management
- Resource coordination

### Connection Pool

Efficient connection management:
- Multiplexed connections per broker
- Automatic connection recovery
- Health checking
- Load balancing

### Protocol Handler

Low-level protocol implementation:
- Binary protocol encoding/decoding
- Request/response correlation
- Error handling
- Compression support

### Buffer Pool

Memory optimization:
- Reusable byte buffers
- Reduced GC pressure
- Configurable pool sizes

## Data Flow

### Producer Flow

```
Application → Producer.Send()
    ↓
Partition Selection
    ↓
Message Serialization
    ↓
Compression (optional)
    ↓
Protocol Encoding
    ↓
Connection Pool → Broker
    ↓
Acknowledgment ← Broker
    ↓
Application Callback
```

### Consumer Flow

```
Broker → Connection Pool
    ↓
Protocol Decoding
    ↓
Decompression (optional)
    ↓
Message Deserialization
    ↓
Application Handler
    ↓
Offset Commit → Broker
```

## Connection Management

### Connection Lifecycle

1. **Initial Connection**
   - DNS resolution
   - TCP connection establishment
   - TLS handshake (if enabled)
   - SASL authentication (if enabled)

2. **Connection Pooling**
   - Connections shared across producers/consumers
   - Idle connection timeout
   - Maximum connections per broker

3. **Health Monitoring**
   - Periodic heartbeats
   - Connection validation
   - Automatic reconnection

### Failure Handling

The SDK implements multiple layers of failure handling:

1. **Connection Failures**
   - Automatic retry with exponential backoff
   - Circuit breaker pattern
   - Fallback to alternate brokers

2. **Request Failures**
   - Configurable retry policies
   - Timeout management
   - Error classification

## Message Flow

### Producer Pipeline

1. **Batching**
   - Accumulate messages up to batch size
   - Flush on timeout or size limit

2. **Compression**
   - Apply configured compression algorithm
   - Header indicates compression type

3. **Partitioning**
   - Hash-based or round-robin
   - Custom partitioner support

### Consumer Pipeline

1. **Fetching**
   - Batch fetch for efficiency
   - Configurable fetch size

2. **Processing**
   - Sequential or concurrent processing
   - Error handling and retry logic

3. **Offset Management**
   - Auto-commit or manual commit
   - Offset storage coordination

## Memory Management

### Zero-Copy Optimizations

- Direct byte buffer usage
- Minimal data copying
- Efficient serialization

### Buffer Pooling

```go
type BufferPool struct {
    small  sync.Pool  // < 4KB
    medium sync.Pool  // 4KB - 64KB
    large  sync.Pool  // > 64KB
}
```

### GC Optimization

- Object reuse patterns
- Minimal allocation in hot paths
- Efficient string handling

## Concurrency Model

### Thread Safety

All public APIs are thread-safe:
- Producer.Send() - concurrent sends
- Consumer.Fetch() - serialized per partition
- Client methods - fully concurrent

### Goroutine Management

```go
// Producer goroutines
- Network I/O worker
- Batch accumulator
- Retry handler

// Consumer goroutines
- Fetch worker
- Offset committer
- Heartbeat sender
```

## Security Architecture

### TLS/mTLS

- Certificate validation
- Cipher suite configuration
- SNI support

### SASL Authentication

Supported mechanisms:
- PLAIN
- SCRAM-SHA-256
- SCRAM-SHA-512

### Encryption

- In-transit encryption via TLS
- Application-level encryption support

## Performance Characteristics

### Throughput

- **Producer**: 1M+ messages/sec
- **Consumer**: 800K+ messages/sec
- Linear scaling with partitions

### Latency

- **P50**: < 1ms
- **P95**: < 5ms
- **P99**: < 10ms

### Resource Usage

- **Memory**: < 50MB baseline
- **CPU**: < 5% idle, scales with load
- **Connections**: 1 per broker minimum

## Protocol Details

### Wire Format

```
┌─────────────┬──────────────┬─────────────┬──────────┐
│ Magic Bytes │ Message Type │ Correlation │  Payload │
│   (4 bytes) │   (2 bytes)  │  (4 bytes)  │ Variable │
└─────────────┴──────────────┴─────────────┴──────────┘
```

### Message Format

```
┌──────────┬──────────┬───────────┬──────────┬─────────┐
│ Headers  │   Key    │   Value   │ Timestamp│  CRC    │
│ Variable │ Variable │ Variable  │ 8 bytes  │ 4 bytes │
└──────────┴──────────┴───────────┴──────────┴─────────┘
```

## Extension Points

### Custom Serializers

```go
type Serializer interface {
    Serialize(data interface{}) ([]byte, error)
    Deserialize(bytes []byte, target interface{}) error
}
```

### Interceptors

```go
type ProducerInterceptor interface {
    OnSend(record *Record) *Record
    OnAcknowledgment(metadata *Metadata, err error)
}
```

### Metrics Collectors

```go
type MetricsCollector interface {
    RecordSend(topic string, latency time.Duration)
    RecordFetch(topic string, partition int, count int)
    RecordError(operation string, err error)
}
```

## Future Enhancements

### Planned Features

1. **Async Consumer API**
   - Channel-based consumption
   - Better concurrency control

2. **Schema Registry**
   - Avro/Protobuf support
   - Schema evolution

3. **Admin Client**
   - Comprehensive topic management
   - ACL configuration

4. **Streaming API**
   - Stream processing primitives
   - Window operations

## Contributing

See our [Contributing Guide](https://github.com/gstreamio/streambus-sdk/blob/main/CONTRIBUTING.md) for:
- Architecture decisions
- Code organization
- Testing requirements
- Performance guidelines