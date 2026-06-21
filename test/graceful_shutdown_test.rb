# frozen_string_literal: true

require "test_helper"

class GracefulShutdownTest < Minitest::Test
  def setup
    SharedBroker.reset_shutdown!
    @in_memory_adapter = SharedBroker::Adapters::InMemory.new
    @client = SharedBroker::Client.new(adapter: @in_memory_adapter)
  end

  def teardown
    SharedBroker.reset_shutdown!
  end

  def test_graceful_shutdown_waits_for_in_flight_message_and_stops_subsequent
    processed_count = 0
    in_flight_processing = false
    finished_processing = false

    @client.subscribe("shutdown.test", "shutdown_queue") do |msg|
      in_flight_processing = true
      sleep 0.2
      processed_count += 1
      finished_processing = true
    end

    t = Thread.new do
      @client.publish("shutdown.test", { id: 1 })
    end

    # Wait for the processing to start
    sleep 0.05
    assert in_flight_processing

    # Trigger shutdown while first message is in flight
    SharedBroker.shutdown!(timeout: 1)

    # Publish a second message - this should NOT be processed because shutdown was requested
    assert_raises(SharedBroker::ShutdownError) do
      @client.publish("shutdown.test", { id: 2 })
    end

    t.join

    assert finished_processing
    assert_equal 1, processed_count
  end
end
