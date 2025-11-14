# Contributing to StreamBus SDK

Thank you for your interest in contributing to the StreamBus Go SDK! We welcome contributions from the community and are grateful for any help you can provide.

## Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct. Please be respectful and considerate in all interactions.

## How to Contribute

### Reporting Issues

1. **Search existing issues** - Check if the issue has already been reported
2. **Create a new issue** - If not found, create a new issue with:
   - Clear, descriptive title
   - Detailed description of the problem
   - Steps to reproduce
   - Expected vs actual behavior
   - SDK version and Go version
   - Any relevant code snippets or error messages

### Suggesting Features

1. **Check existing discussions** - See if the feature has been discussed
2. **Open a discussion** - Start a new discussion in GitHub Discussions
3. **Provide context** - Explain the use case and benefits

### Contributing Code

#### Setup Development Environment

```bash
# Fork the repository on GitHub

# Clone your fork
git clone https://github.com/YOUR_USERNAME/streambus-sdk.git
cd streambus-sdk

# Add upstream remote
git remote add upstream https://github.com/gstreamio/streambus-sdk.git

# Install dependencies
go mod download

# Run tests to verify setup
go test ./...
```

#### Development Workflow

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Follow the coding standards below
   - Add/update tests as needed
   - Update documentation if applicable

3. **Test your changes**
   ```bash
   # Run all tests
   go test ./...

   # Run with race detector
   go test -race ./...

   # Run benchmarks
   go test -bench=. ./...

   # Check coverage
   go test -cover ./...
   ```

4. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat: add new feature X

   Detailed description of what changed and why"
   ```

5. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request**
   - Go to GitHub and create a PR from your fork
   - Fill out the PR template
   - Link any related issues

## Coding Standards

### Go Code Style

- Follow standard Go conventions and idioms
- Use `gofmt` to format your code
- Use `golint` and `go vet` for linting
- Keep line length under 100 characters when possible

```bash
# Format code
gofmt -w .

# Run linter
golint ./...

# Run vet
go vet ./...
```

### Code Organization

```
streambus-sdk/
├── client/           # Public client API
│   ├── client.go
│   ├── producer.go
│   ├── consumer.go
│   └── ...
├── protocol/         # Wire protocol implementation
├── internal/         # Internal packages
├── examples/         # Example applications
└── docs/            # Documentation
```

### Naming Conventions

- **Exported names**: Use PascalCase (e.g., `NewClient`, `ProducerConfig`)
- **Unexported names**: Use camelCase (e.g., `processMessage`, `connectionPool`)
- **Test files**: Name as `*_test.go`
- **Test functions**: Start with `Test` (e.g., `TestProducerSend`)
- **Benchmark functions**: Start with `Benchmark` (e.g., `BenchmarkConsumerFetch`)

### Error Handling

```go
// Define typed errors
var (
    ErrConnectionFailed = errors.New("connection failed")
    ErrInvalidConfig = errors.New("invalid configuration")
)

// Wrap errors with context
if err != nil {
    return fmt.Errorf("failed to connect to broker: %w", err)
}

// Check errors properly
if err != nil {
    // Handle error appropriately
    return err
}
```

### Testing

- Write tests for all new functionality
- Maintain or improve code coverage
- Use table-driven tests where appropriate
- Mock external dependencies

```go
func TestProducerSend(t *testing.T) {
    tests := []struct {
        name    string
        topic   string
        key     []byte
        value   []byte
        wantErr bool
    }{
        {
            name:    "valid message",
            topic:   "test-topic",
            key:     []byte("key"),
            value:   []byte("value"),
            wantErr: false,
        },
        // Add more test cases
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Test implementation
        })
    }
}
```

### Documentation

- Add godoc comments for all exported types and functions
- Include examples in documentation
- Update README if adding new features

```go
// Client represents a connection to StreamBus brokers.
// It manages the lifecycle of connections and provides
// methods for creating producers and consumers.
type Client struct {
    // ...
}

// Send sends a message to the specified topic.
// It returns an error if the message cannot be sent.
//
// Example:
//
//	err := producer.Send("events", []byte("key"), []byte("value"))
//	if err != nil {
//	    log.Fatal(err)
//	}
func (p *Producer) Send(topic string, key, value []byte) error {
    // ...
}
```

## Commit Message Guidelines

We follow the Conventional Commits specification:

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `test`: Test additions or updates
- `build`: Build system changes
- `ci`: CI configuration changes
- `chore`: Other changes

### Examples

```
feat(producer): add batch sending support

Implement batch sending to improve throughput for high-volume
applications. Messages are accumulated up to batch size or timeout
before sending.

Closes #123
```

```
fix(consumer): handle partition rebalance correctly

Fix issue where consumer would lose messages during partition
rebalance by properly committing offsets before rebalancing.

Fixes #456
```

## Pull Request Process

1. **Ensure all tests pass**
2. **Update documentation** as needed
3. **Add tests** for new functionality
4. **Keep PR focused** - One feature/fix per PR
5. **Respond to feedback** promptly
6. **Squash commits** if requested

### PR Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests added/updated
- [ ] No breaking changes (or documented)
```

## Release Process

Releases are managed by maintainers following semantic versioning:

- **Major (X.0.0)**: Breaking changes
- **Minor (0.X.0)**: New features, backwards compatible
- **Patch (0.0.X)**: Bug fixes, backwards compatible

## Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and discussions
- **Documentation**: Check the [docs](https://gstreamio.github.io/streambus-sdk/)

## Recognition

Contributors will be recognized in:
- Release notes
- Contributors file
- GitHub insights

Thank you for contributing to StreamBus SDK!