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

#### A. Event Payload Validation (dry-schema)
Register schemas to validate payload structure automatically on outbound (`publish`) and inbound (`subscribe`) boundaries:

```ruby
user_created_schema = Dry::Schema.Params do
  required(:id).filled(:integer)
  required(:email).filled(:string)
end

SharedBroker::Validation.register("user.created", user_created_schema)
```

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

### Subscribing to Events (Consumer with Retry and DLQ)
To start a persistent event subscriber daemon, register a queue/group name associated with the topic. You can customize the retries and backoff rate:

```ruby
SPOT_BROKER.subscribe("user.created", "my_consumption_queue", max_retries: 3, backoff_base: 2) do |payload|
  puts "Decrypted event successfully validated & consumed! ID: #{payload[:id]}"
  # execute your business logic here...
end
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
