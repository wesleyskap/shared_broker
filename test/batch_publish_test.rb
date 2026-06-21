# frozen_string_literal: true

require "test_helper"

class BatchPublishTest < Minitest::Test
  def setup
    @in_memory_adapter = SharedBroker::Adapters::InMemory.new
    @client = SharedBroker::Client.new(adapter: @in_memory_adapter)
  end

  def test_publish_batch_success
    messages = [
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" }
    ]

    @client.publish_batch("batch.topic", messages, correlation_id: "corr-batch")

    published = @in_memory_adapter.published_messages("batch.topic")
    assert_equal 2, published.size

    msg1 = SharedBroker::Cipher.decrypt(published[0], SharedBroker.encryption_key)
    msg2 = SharedBroker::Cipher.decrypt(published[1], SharedBroker.encryption_key)

    assert_equal 1, msg1[:id]
    assert_equal "Alice", msg1[:name]
    assert_equal "corr-batch", msg1[:_correlation_id]

    assert_equal 2, msg2[:id]
    assert_equal "Bob", msg2[:name]
    assert_equal "corr-batch", msg2[:_correlation_id]
  end

  def test_publish_batch_validation_failure
    schema = Dry::Schema.Params do
      required(:id).filled(:integer)
    end
    SharedBroker::Validation.register("validated.batch", schema)

    messages = [
      { id: 1 },
      { id: "invalid-id" }
    ]

    assert_raises(SharedBroker::Validation::ValidationError) do
      @client.publish_batch("validated.batch", messages)
    end

    published = @in_memory_adapter.published_messages("validated.batch")
    assert_empty published # The transaction failed early on validation of the second message

    SharedBroker::Validation.clear
  end

  def test_publish_batch_argument_error
    assert_raises(ArgumentError) do
      @client.publish_batch("topic", "not-an-array")
    end
  end
end
