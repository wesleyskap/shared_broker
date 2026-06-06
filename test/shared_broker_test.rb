# frozen_string_literal: true

require "test_helper"

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
    assert_equal "created", published.first[:event]
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
end
