# frozen_string_literal: true

require "test_helper"

class ActiveJobTest < Minitest::Test
  def setup
    @adapter = SharedBroker::Adapters::ActiveJob.new
    @client = SharedBroker::Client.new(adapter: @adapter)
    SharedBroker::Adapters::ActiveJob.handlers.clear
  end

  def test_active_job_publish_and_subscribe_flow
    received = []
    @client.subscribe("order.shipped", "job_queue") do |msg|
      received << msg
    end

    # Publish an event - in non-rails fallback mode it runs inline/async thread
    t = @client.publish("order.shipped", { id: 999 }, correlation_id: "corr-aj")
    t.join if t.is_a?(Thread)

    assert_equal 1, received.size
    assert_equal 999, received.first[:id]
    assert_equal "corr-aj", received.first[:_correlation_id]
  end
end
