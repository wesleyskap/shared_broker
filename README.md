# SharedBroker

`SharedBroker` is a high-performance Ruby library designed to simplify event-based communication (asynchronous messaging) and telemetry (observability) in Rails microservice architectures.

The library implements the **Adapter Pattern** to decouple your application from physical queue providers (like RabbitMQ), allowing easy broker swapping and clean synchronous testing with an in-memory adapter.

---

## Key Features

- **Pluggable Messaging**: Adapter pattern to decouple Rails from physical messaging queues.
- **RabbitMQ Adapter**: Robust, persistent connection wrapper using the `bunny` gem.
- **InMemory Adapter**: Synchronous local queue simulation for fast TDD testing (no inline external I/O stubs required).
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

Create an initializer in your Rails application (`config/initializers/shared_broker.rb`):

```ruby
require "shared_broker"

# 1. Configure the Adapter based on Environment
if Rails.env.test?
  # In-memory adapter prevents external queue dependency during unit tests
  BROKER_ADAPTER = SharedBroker::Adapters::InMemory.new
else
  # Connects to real RabbitMQ broker
  amqp_url = ENV.fetch("RABBITMQ_URL") { "amqp://guest:guest@localhost:5672" }
  BROKER_ADAPTER = SharedBroker::Adapters::RabbitMQ.new(amqp_url: amqp_url)
end

# 2. Instantiate the Client by Injecting the Adapter
SPOT_BROKER = SharedBroker::Client.new(adapter: BROKER_ADAPTER)

# 3. Initialize Telemetry (OpenTelemetry)
SharedBroker::Telemetry.configure(service_name: "my_microservice")
```

---

## Usage

### Publishing Events
Send simple events by passing the topic name and a structured payload (must be a `Hash`):

```ruby
event_data = {
  id: 1,
  name: "Eiffel Tower",
  latitude: 48.8584,
  longitude: 2.2945
}

SPOT_BROKER.publish("spot.created", event_data)
```

### Subscribing to Events (Consumer)
To start a persistent event subscriber daemon, register a queue associated with the topic:

```ruby
SPOT_BROKER.subscribe("spot.created", "my_consumption_queue") do |payload|
  puts "Event successfully consumed! ID: #{payload[:id]}"
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
