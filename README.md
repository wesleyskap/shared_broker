[![Gem Version](https://badge.fury.io/rb/shared_broker.svg?icon=si%3Arubygems)](https://badge.fury.io/rb/shared_broker)

# SharedBroker

`SharedBroker` is a high-performance Ruby library designed to simplify event-based communication (asynchronous messaging) and telemetry (observability) in Rails microservice architectures.

The library implements the **Adapter Pattern** to decouple your application from physical queue providers, allowing easy broker swapping and clean synchronous testing with an in-memory adapter.

---

## Key Features

- **Pluggable Messaging**: Adapter pattern supporting:
  - `InMemory`: Synchronous local simulation for fast TDD testing (no inline external I/O stubs required).
  - `RabbitMQ`: Robust connection using the `bunny` gem.
  - `Kafka`: High-throughput adapter using the `kafka` gem.
  - `Redis`: Light-weight Pub/Sub broker using the `redis` gem.
- **Resilience & Fault Tolerance**:
  - **Automatic Retry**: Automatic retry mechanism on message processing failures using exponential backoff.
  - **Dead Letter Queue (DLQ)**: Messages that exhaust their retries are automatically moved to a DLQ (`#{queue_name}.dlq` or a custom topic/list depending on the adapter) containing error metadata headers.
  - **Circuit Breaker**: Integrated thread-safe Circuit Breaker wrapping message publication to prevent cascading failures.
- **Security & Data Validation**:
  - **Strict Schema Validation**: Integration with `dry-schema` to validate message structures on both publish (boundaries out) and subscribe (boundaries in).
  - **Transparent Payload Encryption**: Payloads are automatically encrypted at rest using AES-256-GCM via `SharedBroker.encryption_key`.
- **Integrated OpenTelemetry**: Centralized SDK configuration with auto-instrumentation for all supported libraries (ActiveRecord, Bunny, Faraday, Rails, PG, etc.).

---

## Installation

Add this line to your application's `Gemfile`:

```ruby
gem "shared_broker", path: "gems/shared_broker" # for local gem
# or when published:
# gem "shared_broker"
```

And execute:

```bash
bundle install
```

---

## Configuration

Create an initializer in your Rails application (`config/initializers/shared_broker.rb`). Below is the breakdown of what is **required** versus what is **optional**.

### 1. Required Configuration (Minimum Setup)

You must configure the adapter depending on the environment, initialize the client, and configure the payload encryption key (required since AES-256-GCM is active by default):

```ruby
require "shared_broker"

# A. Configure Payload Encryption Key (AES-256-GCM)
# Expects a 32-byte string. Use a secure production key in production.
SharedBroker.encryption_key = ENV.fetch("SHARED_BROKER_ENCRYPTION_KEY") { "a" * 32 }

# B. Configure the Adapter based on Environment
if Rails.env.test?
  # In-memory adapter prevents external queue dependency during unit tests
  BROKER_ADAPTER = SharedBroker::Adapters::InMemory.new
else
  # Connects to real RabbitMQ broker
  amqp_url = ENV.fetch("RABBITMQ_URL") { "amqp://guest:guest@localhost:5672" }
  BROKER_ADAPTER = SharedBroker::Adapters::RabbitMQ.new(amqp_url: amqp_url)
end

# C. Instantiate the Client by Injecting the Adapter
SPOT_BROKER = SharedBroker::Client.new(adapter: BROKER_ADAPTER)
```

---

### 2. Optional Configuration

These features can be configured optionally depending on your needs.

#### A. Event Payload Validation & Schema Registry

`SharedBroker` supports validation on outbound (`publish`) and inbound (`subscribe`) boundaries. It includes a pluggable **Schema Registry** supporting both local definitions and remote registry servers.

##### A1. Local Validation (dry-schema - default)
Register schemas to validate payload structure locally using `dry-schema`:

```ruby
user_created_schema = Dry::Schema.Params do
  required(:id).filled(:integer)
  required(:email).filled(:string)
end

# Registered on the default local provider
SharedBroker::Validation.register("user.created", user_created_schema)
```

##### A2. Http Schema Registry (JSON Schema)
Configure `SharedBroker` to fetch schemas dynamically from an HTTP-based Schema Registry. The HTTP provider validates payloads using the standard JSON Schema specification and caches schemas in-memory to prevent validation latency:

```ruby
# Configure the HTTP Schema Registry provider
SharedBroker::SchemaRegistry.provider = SharedBroker::SchemaRegistry::Providers::Http.new(
  url: "https://schema-registry.mycorp.internal",
  headers: { "Authorization" => "Bearer my-secret-token" },
  cache_ttl: 300 # cache schemas in-memory for 5 minutes
)
```
When configured with the HTTP provider, any published or subscribed event will automatically trigger a lookup against `https://schema-registry.mycorp.internal/schemas/{topic}.json`.


#### B. Custom Circuit Breaker
By default, the client instantiates a standard Circuit Breaker. You can provide a custom one to tune the failure threshold and recovery window:

```ruby
custom_circuit_breaker = SharedBroker::CircuitBreaker.new(
  failure_threshold: 5,   # trip circuit after 5 failures
  recovery_timeout: 30    # wait 30 seconds before attempting recovery
)

SPOT_BROKER = SharedBroker::Client.new(
  adapter: BROKER_ADAPTER,
  circuit_breaker: custom_circuit_breaker
)
```

#### C. Initialize Distributed Tracing (OpenTelemetry)
Initialize the OpenTelemetry SDK with auto-instrumentation for the microservice:

```ruby
SharedBroker::Telemetry.configure(service_name: "my_microservice")
```

#### D. Hybrid Multi-Adapter Routing
If your system requires directing different topics to different message brokers (e.g., Kafka for telemetry, RabbitMQ for transactional events, Redis for quick cache invalidation), you can configure `SharedBroker::Client` with multiple adapters and a routing table:

```ruby
# 1. Initialize physical adapters
rabbit_adapter = SharedBroker::Adapters::RabbitMQ.new(amqp_url: "amqp://guest:guest@localhost:5672")
kafka_adapter  = SharedBroker::Adapters::Kafka.new(seed_brokers: ["localhost:9092"])
redis_adapter  = SharedBroker::Adapters::Redis.new(redis_url: "redis://localhost:6379")

# 2. Instantiate the Client in Hybrid mode
HYBRID_BROKER = SharedBroker::Client.new(
  adapters: {
    rabbitmq: rabbit_adapter,
    kafka:    kafka_adapter,
    redis:    redis_adapter
  },
  routing: {
    # Exact routing match
    "payment.processed" => :rabbitmq,
    # Wildcard routing match
    "telemetry.*"       => :kafka,
    "cache.*"           => :redis,
    # Default fallback routing
    "*"                 => :rabbitmq
  }
)
```

#### E. Encryption Key Rotation & Granularity

If your system requires encrypting payloads with different keys based on the topic (e.g., highly sensitive financial data vs. general notifications) or rotating keys without breaking the decryption of historical messages in queues, you can configure a Key Provider Registry:

```ruby
# 1. Initialize Registry with a map of historical/current keys and active key mappings
key_registry = SharedBroker::KeyProvider::Registry.new(
  keys: {
    "v1"            => "a" * 32, # historical key
    "v2"            => "b" * 32, # current general key
    "finance_key_1" => "c" * 32  # current finance key
  },
  active_keys: {
    # Topic-specific key mapping using glob patterns
    "payment.*" => "finance_key_1",
    # Fallback key mapping for all other topics
    "*"         => "v2"
  }
)

# 2. Register key provider globally or pass it to Client initialize
SharedBroker.key_provider = key_registry

# Alternatively, pass it directly to the client
SPOT_BROKER = SharedBroker::Client.new(
  adapter: BROKER_ADAPTER,
  key_provider: key_registry
)
```

With a key provider registry configured:
- **Publishing**: Payloads are encrypted using the active key matching the topic pattern, and a `_key_id` metadata tag is automatically appended to the envelope.
- **Subscribing**: The gem automatically reads the `_key_id` from the payload envelope and decrypts it using the correct historical key version.
- **Fallback**: If no `_key_id` is present on a received message (e.g., legacy message), it falls back to the key associated with the topic pattern.

#### F. Automatic Payload Compression

To optimize network bandwidth and storage costs, `SharedBroker` can automatically compress large payloads before encrypting them. You can configure compression globally:

```ruby
# Enable compression using either :gzip or :deflate (nil by default)
SharedBroker.compression_algorithm = :gzip

# Set the threshold in bytes. Payloads smaller than this will NOT be compressed.
# This avoids the overhead of compression for tiny messages.
SharedBroker.compression_threshold = 1024 # 1 KB
```

When compression is active:
- **Publishing**: If the payload size exceeds the threshold, it is compressed, marked with the `_compression` tag in the metadata envelope, and then encrypted.
- **Subscribing**: The consumer checks for the `_compression` tag, decrypts the payload, and automatically decompresses it before passing it to your subscriber block.
- **Compatibility**: If a consumer receives a message that has no `_compression` tag, it will bypass decompression and decrypt normally.

---

## Usage

### Publishing Events
Send events by passing the topic name and a structured payload (must be a `Hash`):

```ruby
event_data = {
  id: 1,
  email: "test@example.com"
}

# The payload will be validated against its dry-schema, encrypted, and published safely.
SPOT_BROKER.publish("user.created", event_data)
```

### Subscribing to Events (Consumer with Retry, DLQ, Concurrency & Backpressure)
To start a persistent event subscriber daemon, register a queue/group name associated with the topic. You can customize the retries, backoff rate, concurrency limits, and dynamic backpressure checks:

```ruby
# Basic subscriber
SPOT_BROKER.subscribe("user.created", "my_consumption_queue", max_retries: 3, backoff_base: 2) do |payload|
  puts "Decrypted event successfully validated & consumed! ID: #{payload[:id]}"
  # execute your business logic here...
end

# Advanced subscriber with Concurrency limits and Dynamic Backpressure
SPOT_BROKER.subscribe(
  "user.created",
  "my_consumption_queue",
  max_concurrency: 5,                       # Process up to 5 events concurrently
  backpressure_check: -> { db_overloaded? }, # Stop pulling new events if true
  backpressure_backoff: 2.0                 # Check health again after 2 seconds
) do |payload|
  # business logic running within the concurrency wrapper
end
```


### Publishing Events in Batch
Send multiple events at once:

```ruby
events = [
  { id: 1, email: "alice@example.com" },
  { id: 2, email: "bob@example.com" }
]

# Validates and encrypts all payloads, then publishes them in a batch
SPOT_BROKER.publish_batch("user.created", events)
```

### Subscriber Idempotency Middleware
Configure deduplication of messages using the Idempotency Middleware. It skips already processed messages by checking their `correlation_id` (uses an internal in-memory store by default, but accepts duck-typed stores like `Rails.cache`):

```ruby
# Initialize with custom store and cache expiration (default 3600 seconds)
idempotency = SharedBroker::Middlewares::Idempotency.new(
  store: Rails.cache,
  expires_in: 86400 # 1 day
)

SPOT_BROKER = SharedBroker::Client.new(
  adapter: BROKER_ADAPTER,
  middlewares: [idempotency]
)
```

### DLQ Redrive Utility
Move failed messages back to the original queue for reprocessing after bug fixes:

```ruby
# Redrives up to 50 messages from DLQ list/queue to original topic
SharedBroker::DLQ::Redriver.redrive(
  SPOT_BROKER, 
  "my_consumption_queue.dlq", 
  "user.created", 
  limit: 50
)
```

---

## Running Gem Tests

To run the unit test suite using **Minitest**:

```bash
bundle exec rake test
```

---

## License

This Gem is available under the terms of the [MIT License](https://opensource.org/licenses/MIT).
