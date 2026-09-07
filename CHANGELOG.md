# Changelog

## [0.2.1](https://github.com/gstreamio/streambus-sdk/compare/v0.1.1...v0.2.1) (2026-09-06)

Contents are the 0.2.0 release. The v0.2.0 tag was deleted after publication
and GitHub permanently reserves tag names from deleted immutable releases, so
that number cannot be re-cut; 0.2.1 carries the same work. Anything already
resolving `v0.2.0` through the Go module proxy keeps working and is
equivalent - only the changelog and the release-please configuration differ.


### ⚠ BREAKING CHANGES

* every client call that performs network I/O now takes a context.Context as its first argument - CreateTopic, DeleteTopic, ListTopics, Producer.Send, Producer.Flush, Consumer.Fetch, Consumer.FetchOne and Consumer.SeekToEnd among them. This comes from syncing the vendored packages with the core repository, where context propagation was threaded through the client. Callers must pass a context; there is no non-context overload.

### Features

* sync vendored packages with streambus main, add CI, retire the Report Card badge ([baf920e](https://github.com/gstreamio/streambus-sdk/commit/baf920e7))

### Bug Fixes

* tag releases without a component prefix, so the Go toolchain can resolve them ([a4e9f92](https://github.com/gstreamio/streambus-sdk/commit/a4e9f92c))

### Continuous Integration

* add release-please so the SDK actually cuts releases ([598f273](https://github.com/gstreamio/streambus-sdk/commit/598f273e))

### Documentation

* repoint the changelog at the commits reachable from main ([90de5d6](https://github.com/gstreamio/streambus-sdk/commit/90de5d60))
