# Changelog

## [0.3.0](https://github.com/gstreamio/streambus-sdk/compare/v0.2.0...v0.3.0) (2026-09-06)


### ⚠ BREAKING CHANGES

* every client call that performs network I/O now takes a context.Context as its first argument - CreateTopic, DeleteTopic, ListTopics, Producer.Send, Producer.Flush, Consumer.Fetch, Consumer.FetchOne and Consumer.SeekToEnd among them. This comes from syncing the vendored packages with the core repository, where context propagation was threaded through the client. Callers must pass a context; there is no non-context overload.

### Features

* sync vendored packages with streambus main, add CI, retire the Report Card badge ([24591a5](https://github.com/gstreamio/streambus-sdk/commit/24591a5b45ad6e0e97284ac6c1ceda02bdeb6814))
* sync vendored packages with streambus main, add CI, retire the Report Card badge ([baf920e](https://github.com/gstreamio/streambus-sdk/commit/baf920e730fea8e78361a192219037f330dd2857))


### Bug Fixes

* **ci:** tag releases v0.2.0, not streambus-sdk-v0.2.0 ([#7](https://github.com/gstreamio/streambus-sdk/issues/7)) ([a4e9f92](https://github.com/gstreamio/streambus-sdk/commit/a4e9f92c3bde6d44f7699637afc5bc0918fc2c87))


### Continuous Integration

* add release-please so the SDK actually cuts releases ([598f273](https://github.com/gstreamio/streambus-sdk/commit/598f273e8cde15809ab07898c953033d347a4618))

## [0.2.0](https://github.com/gstreamio/streambus-sdk/compare/streambus-sdk-v0.1.1...streambus-sdk-v0.2.0) (2026-09-06)


### ⚠ BREAKING CHANGES

* every client call that performs network I/O now takes a context.Context as its first argument - CreateTopic, DeleteTopic, ListTopics, Producer.Send, Producer.Flush, Consumer.Fetch, Consumer.FetchOne and Consumer.SeekToEnd among them. This comes from syncing the vendored packages with the core repository, where context propagation was threaded through the client. Callers must pass a context; there is no non-context overload.

### Features

* sync vendored packages with streambus main, add CI, retire the Report Card badge ([2604fb5](https://github.com/gstreamio/streambus-sdk/commit/2604fb53d78b4979c3f42951734d4cf4c33dfcf5))
* sync vendored packages with streambus main, add CI, retire the Report Card badge ([3a1396c](https://github.com/gstreamio/streambus-sdk/commit/3a1396c111e57a7f1949ecdc0e2199120ddae4bf))


### Continuous Integration

* add release-please so the SDK actually cuts releases ([7501e7c](https://github.com/gstreamio/streambus-sdk/commit/7501e7cfeea030884feb5ffee8eb164322e6d32a))
