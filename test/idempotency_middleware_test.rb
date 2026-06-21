# frozen_string_literal: true

require "test_helper"

class IdempotencyMiddlewareTest < Minitest::Test
  class MockCache
    attr_reader :store
    def initialize
      @store = {}
    end

    def exist?(key)
      @store.key?(key)
    end

    def write(key, value, expires_in: 3600)
      @store[key] = { value: value, expires_in: expires_in }
    end
  end

  def setup
    @in_memory_adapter = SharedBroker::Adapters::InMemory.new
  end

  def test_idempotency_deduplicates_messages
    store = MockCache.new
    idempotency_middleware = SharedBroker::Middlewares::Idempotency.new(store: store, expires_in: 60)
    
    client = SharedBroker::Client.new(
      adapter: @in_memory_adapter,
      middlewares: [idempotency_middleware]
    )

    processed_count = 0
    client.subscribe("user.events", "user_queue") do |msg|
      processed_count += 1
    end

    # First message publication
    client.publish("user.events", { data: "first" }, correlation_id: "corr-unique-1")
    assert_equal 1, processed_count

    # Duplicate message publication (same correlation_id)
    # The subscription block should not run again
    client.publish("user.events", { data: "duplicate" }, correlation_id: "corr-unique-1")
    assert_equal 1, processed_count

    # Different correlation_id
    client.publish("user.events", { data: "different" }, correlation_id: "corr-unique-2")
    assert_equal 2, processed_count
  end

  def test_idempotency_with_default_memorystore
    idempotency_middleware = SharedBroker::Middlewares::Idempotency.new(expires_in: 0.1)
    
    client = SharedBroker::Client.new(
      adapter: @in_memory_adapter,
      middlewares: [idempotency_middleware]
    )

    processed_count = 0
    client.subscribe("test.idempotency", "test_queue") do |msg|
      processed_count += 1
    end

    client.publish("test.idempotency", { val: 1 }, correlation_id: "corr-idemp")
    assert_equal 1, processed_count

    # Duplicate gets ignored
    client.publish("test.idempotency", { val: 1 }, correlation_id: "corr-idemp")
    assert_equal 1, processed_count

    # Sleep so MemoryStore prunes expired keys (expires_in is 0.1)
    sleep 0.15

    # Should process again after key expires
    client.publish("test.idempotency", { val: 1 }, correlation_id: "corr-idemp")
    assert_equal 2, processed_count
  end
end
