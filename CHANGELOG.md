# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - Unreleased

### Added
- Phase 4: Pipeline of customizable Middlewares (Interceptors) for publishing and subscribing messages.
- Phase 4: Distributed Tracing using W3C Trace Context (`traceparent`/`tracestate`) injection and extraction.

## [1.0.0] - 2026-06-06

### Added
- Phase 1: Fault Tolerance & Resilience features:
  - Exponential backoff retry loop for message processing.
  - Automatic Dead Letter Queue (DLQ) routing with rich metadata headers (`x_failed_at`, `x_exception_class`, `x_exception_message`, `x_original_routing_key`).
  - Thread-safe `CircuitBreaker` wrapping all outbound publisher calls.
- Phase 2: Schema Validation and Security:
  - Outbound/Inbound boundary validation using `dry-schema`.
  - Transparent AES-256-GCM symmetric payload encryption by default, configurable with `SharedBroker.encryption_key`.
- Phase 3: Scalable Adapters:
  - Apache Kafka adapter (`SharedBroker::Adapters::Kafka`) with dynamic dependency loading.
  - Redis Pub/Sub adapter (`SharedBroker::Adapters::Redis`) with list-based DLQ routing.
- Comprehensive test coverage for all the above features using isolated fakes and Minitest.

## [0.1.0] - 2026-06-06

### Added
- Initial release with the pluggable `Client` messaging system.
- `InMemory` adapter for local testing.
- `RabbitMQ` adapter using the `bunny` gem.
- Basic OpenTelemetry instrumentation utility (`SharedBroker::Telemetry`).
