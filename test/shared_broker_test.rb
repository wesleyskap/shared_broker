# frozen_string_literal: true

require "test_helper"

module MockKafka
  class Client
    attr_reader :delivered_messages
    def initialize(*args); @delivered_messages = []; end
    def deliver_message(value, topic:, headers: {})
      @delivered_messages << { value: value, topic: topic, headers: headers }
    end
    def consumer(group_id:); Consumer.new(self); end
  end

  class Consumer
    def initialize(client); @client = client; end
    def subscribe(topic); @topic = topic; end
    def each_message; yield Message.new({ key: "val" }.to_json, { "correlation_id" => "corr-kafka" }); end
  end

  class Message
    attr_reader :value, :headers
    def initialize(value, headers); @value = value; @headers = headers; end
  end
end

module MockRedis
  class Client
    attr_reader :published, :lists
    def initialize(*args)
      @published = []
      @lists = Hash.new { |h, k| h[k] = [] }
    end
    def publish(channel, message); @published << { channel: channel, message: message }; end
    def subscribe(channel); yield Subscription.new(self, channel); end
    def rpush(key, value); @lists[key] << value; end
  end

  class Subscription
    def initialize(client, channel); @client = client; @channel = channel; end
    def message; yield @channel, { key: "val" }.to_json; end
  end
end

unless defined?(::Kafka)
  module ::Kafka
    def self.new(*args); MockKafka::Client.new; end
  end
end

unless defined?(::Redis)
  module ::Redis
    def self.new(*args); MockRedis::Client.new; end
  end
end

class SharedBrokerTest < Minitest::Test
  def setup
    @in_memory_adapter = SharedBroker::Adapters::InMemory.new
    @client = SharedBroker::Client.new(adapter: @in_memory_adapter)
  end

  def test_that_it_has_a_version_number
    refute_nil ::SharedBroker::VERSION
  end

  def test_publish_message
    @client.publish("test.topic", { event: "created", id: 1 })
    published = @in_memory_adapter.published_messages("test.topic")
    assert_equal 1, published.size
    decrypted = SharedBroker::Cipher.decrypt(published.first, SharedBroker.encryption_key)
    assert_equal "created", decrypted[:event]
  end

  def test_publish_message_with_correlation_id
    @client.publish("test.topic", { event: "created" }, correlation_id: "corr-123")
    published = @in_memory_adapter.published_messages("test.topic")
    decrypted = SharedBroker::Cipher.decrypt(published.first, SharedBroker.encryption_key)
    assert_equal "corr-123", decrypted[:_correlation_id]
  end

  def test_subscribe_message
    received = []
    @client.subscribe("test.topic", "test_queue") do |msg|
      received << msg
    end

    @client.publish("test.topic", { payload: "hello" })
    assert_equal 1, received.size
    assert_equal "hello", received.first[:payload]
  end

  def test_invalid_adapter_raises_error
    error = assert_raises(ArgumentError) do
      SharedBroker::Client.new(adapter: nil)
    end
    assert_match(/Expected adapter to respond to :publish/, error.message)
  end

  def test_telemetry_configuration_with_invalid_name
    assert_raises(ArgumentError) do
      SharedBroker::Telemetry.configure(service_name: "")
    end
  end

  def test_circuit_breaker_trips_and_resets
    cb = SharedBroker::CircuitBreaker.new(failure_threshold: 2, recovery_timeout: 0.1)
    client = SharedBroker::Client.new(adapter: @in_memory_adapter, circuit_breaker: cb)

    bad_adapter = Object.new
    def bad_adapter.publish(*args); raise "network error"; end
    def bad_adapter.subscribe(*args); end
    client_bad = SharedBroker::Client.new(adapter: bad_adapter, circuit_breaker: cb)

    assert_raises(RuntimeError) { client_bad.publish("topic", { data: 1 }) }
    assert_raises(RuntimeError) { client_bad.publish("topic", { data: 1 }) }

    assert_equal :open, cb.state
    assert_raises(SharedBroker::CircuitBreaker::OpenError) { client_bad.publish("topic", { data: 1 }) }

    sleep 0.12
    client.publish("topic", { data: "success" })
    assert_equal :closed, cb.state
  end

  def test_subscriber_retry_and_dlq
    received_attempts = 0
    @client.subscribe("retry.topic", "test_queue_retry", max_retries: 2, backoff_base: 2) do |msg|
      received_attempts += 1
      raise "processing error"
    end

    @client.publish("retry.topic", { payload: "retry_me" })

    assert_equal 3, received_attempts

    dlq_messages = @in_memory_adapter.published_messages("test_queue_retry.dlq")
    assert_equal 1, dlq_messages.size
    decrypted_dlq = SharedBroker::Cipher.decrypt(dlq_messages.first, SharedBroker.encryption_key)
    assert_equal "retry_me", decrypted_dlq[:payload]
    assert_equal "retry.topic", decrypted_dlq[:_x_original_routing_key]
    assert_equal "RuntimeError", decrypted_dlq[:_x_exception_class]
    assert_equal "processing error", decrypted_dlq[:_x_exception_message]
  end

  def test_validation_success_and_failure
    schema = Dry::Schema.Params do
      required(:id).filled(:integer)
      required(:name).filled(:string)
    end
    
    SharedBroker::Validation.register("test.validated", schema)
    
    client = SharedBroker::Client.new(adapter: @in_memory_adapter)
    client.publish("test.validated", { id: 42, name: "Alice" })
    
    error = assert_raises(SharedBroker::Validation::ValidationError) do
      client.publish("test.validated", { id: "not-an-integer" })
    end
    assert_match(/Schema validation failed for topic/, error.message)
    assert_match(/:id/, error.message)
    assert_match(/:name/, error.message)
    
    SharedBroker::Validation.clear
  end

  def test_encryption_and_decryption
    key = "b" * 32
    SharedBroker.encryption_key = key
    
    client = SharedBroker::Client.new(adapter: @in_memory_adapter)
    
    received = []
    client.subscribe("crypto.topic", "crypto_queue") do |msg|
      received << msg
    end
    
    client.publish("crypto.topic", { secret: "sensitive data" })
    
    raw_published = @in_memory_adapter.published_messages("crypto.topic").first
    assert raw_published[:_encrypted]
    refute raw_published.key?(:secret)
    refute_nil raw_published[:_iv]
    refute_nil raw_published[:_auth_tag]
    
    assert_equal 1, received.size
    assert_equal "sensitive data", received.first[:secret]
    
    SharedBroker.encryption_key = "a" * 32
  end

  def test_kafka_adapter_publish
    kafka_mock = MockKafka::Client.new
    ::Kafka.define_singleton_method(:new) { |*args| kafka_mock } rescue nil
    
    adapter = SharedBroker::Adapters::Kafka.new(seed_brokers: ["localhost:9092"])
    client = SharedBroker::Client.new(adapter: adapter)
    
    client.publish("kafka.topic", { data: 123 }, correlation_id: "corr-1")
    
    assert_equal 1, kafka_mock.delivered_messages.size
    assert_equal "kafka.topic", kafka_mock.delivered_messages.first[:topic]
    assert_equal "corr-1", kafka_mock.delivered_messages.first[:headers]["correlation_id"]
  end

  def test_redis_adapter_publish
    redis_mock = MockRedis::Client.new
    ::Redis.define_singleton_method(:new) { |*args| redis_mock } rescue nil
    
    adapter = SharedBroker::Adapters::Redis.new(redis_url: "redis://localhost:6379")
    client = SharedBroker::Client.new(adapter: adapter)
    
    client.publish("redis.topic", { data: 456 }, correlation_id: "corr-2")
    
    assert_equal 1, redis_mock.published.size
    assert_equal "redis.topic", redis_mock.published.first[:channel]
  end
end
