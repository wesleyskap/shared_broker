# frozen_string_literal: true

require "test_helper"

class DqRedriveTest < Minitest::Test
  def setup
    @in_memory_adapter = SharedBroker::Adapters::InMemory.new
    @client = SharedBroker::Client.new(adapter: @in_memory_adapter)
  end

  def test_dlq_redrive_success
    attempts = 0
    # Setup a subscriber that fails first 2 times, then succeeds
    @client.subscribe("order.created", "order_queue", max_retries: 0) do |msg|
      attempts += 1
      raise "temporary database error" if msg[:fail]
    end

    # Publish message that will fail and go to DLQ
    @client.publish("order.created", { id: 100, fail: true })

    dlq_messages = @in_memory_adapter.published_messages("order_queue.dlq")
    assert_equal 1, dlq_messages.size

    # Update subscriber to not fail
    @client = SharedBroker::Client.new(adapter: @in_memory_adapter)
    received = []
    @client.subscribe("order.created", "order_queue") do |msg|
      received << msg
    end

    # Run redrive
    SharedBroker::DLQ::Redriver.redrive(@client, "order_queue.dlq", "order.created")

    # The DLQ should be empty
    assert_empty @in_memory_adapter.published_messages("order_queue.dlq")
    
    # Message should be delivered to the new subscription
    assert_equal 1, received.size
    assert_equal 100, received.first[:id]
  end

  def test_dlq_redrive_with_limit
    @in_memory_adapter.publish("dlq_queue.dlq", { id: 1, fail: true })
    @in_memory_adapter.publish("dlq_queue.dlq", { id: 2, fail: true })
    @in_memory_adapter.publish("dlq_queue.dlq", { id: 3, fail: true })

    assert_equal 3, @in_memory_adapter.published_messages("dlq_queue.dlq").size

    # Redrive only 2 messages
    SharedBroker::DLQ::Redriver.redrive(@client, "dlq_queue.dlq", "original.topic", limit: 2)

    assert_equal 1, @in_memory_adapter.published_messages("dlq_queue.dlq").size
    assert_equal 2, @in_memory_adapter.published_messages("original.topic").size
  end
end
