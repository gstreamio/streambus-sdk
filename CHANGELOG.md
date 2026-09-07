# Changelog

## [0.2.0](https://github.com/gstreamio/streambus-sdk/compare/v0.1.1...v0.2.0) (2026-09-06)


### ⚠ BREAKING CHANGES

* every client call that performs network I/O now takes a context.Context as its first argument - CreateTopic, DeleteTopic, ListTopics, Producer.Send, Producer.Flush, Consumer.Fetch, Consumer.FetchOne and Consumer.SeekToEnd among them. This comes from syncing the vendored packages with the core repository, where context propagation was threaded through the client. Callers must pass a context; there is no non-context overload.

### Features

* sync vendored packages with streambus main, add CI, retire the Report Card badge ([baf920e](https://github.com/gstreamio/streambus-sdk/commit/baf920e7))

### Bug Fixes

* tag releases v0.2.0, not streambus-sdk-v0.2.0 ([a4e9f92](https://github.com/gstreamio/streambus-sdk/commit/a4e9f92c))

### Continuous Integration

* add release-please so the SDK actually cuts releases ([598f273](https://github.com/gstreamio/streambus-sdk/commit/598f273e))
