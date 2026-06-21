# frozen_string_literal: true

require "test_helper"
require "ostruct"

class AwsSqsSnsTest < Minitest::Test
  class FakeSnsClient
    attr_reader :publishes
    def initialize
      @publishes = []
    end

    def publish(topic_arn:, message:)
      @publishes << { topic_arn: topic_arn, message: message }
      Object.new
    end
  end

  class FakeSqsClient
    attr_reader :messages, :deletions, :sent_messages
    def initialize(messages = [])
      @messages = messages
      @deletions = []
      @sent_messages = []
    end

    def receive_message(queue_url:, max_number_of_messages:, wait_time_seconds:)
      msgs = @messages.shift(max_number_of_messages)
      OpenStruct.new(messages: msgs || [])
    end

    def delete_message(queue_url:, receipt_handle:)
      @deletions << { queue_url: queue_url, receipt_handle: receipt_handle }
    end

    def send_message(queue_url:, message_body:)
      @sent_messages << { queue_url: queue_url, body: message_body }
    end
  end

  class FakeMessage
    attr_reader :body, :receipt_handle
    def initialize(body, receipt_handle = "handle-123")
      @body = body
      @receipt_handle = receipt_handle
    end
  end

  def test_publish_success
    sns = FakeSnsClient.new
    sqs = FakeSqsClient.new
    adapter = SharedBroker::Adapters::AwsSqsSns.new(sqs_client: sqs, sns_client: sns)
    client = SharedBroker::Client.new(adapter: adapter)

    client.publish("arn:aws:sns:us-east-1:123456789012:topic", { event: "sent" }, correlation_id: "corr-aws")

    assert_equal 1, sns.publishes.size
    published = sns.publishes.first
    assert_equal "arn:aws:sns:us-east-1:123456789012:topic", published[:topic_arn]
    
    decrypted = SharedBroker::Cipher.decrypt(JSON.parse(published[:message], symbolize_names: true), SharedBroker.encryption_key)
    assert_equal "sent", decrypted[:event]
    assert_equal "corr-aws", decrypted[:_correlation_id]
  end

  def test_subscribe_success_and_delete
    sns = FakeSnsClient.new
    
    # Preload a valid encrypted message in SQS fake client
    encrypted_msg = SharedBroker::Cipher.encrypt({ data: "payload" }, SharedBroker::KeyProvider::Static.new(SharedBroker.encryption_key))
    raw_message = FakeMessage.new(encrypted_msg.to_json)
    
    sqs = FakeSqsClient.new([raw_message])
    adapter = SharedBroker::Adapters::AwsSqsSns.new(sqs_client: sqs, sns_client: sns)
    client = SharedBroker::Client.new(adapter: adapter)

    received = []
    thread = client.subscribe("topic", "https://sqs.us-east-1.amazonaws.com/123/queue") do |msg|
      received << msg
    end

    # Wait for the poller thread to process the preloaded message
    sleep 0.1

    assert_equal 1, received.size
    assert_equal "payload", received.first[:data]
    assert_equal 1, sqs.deletions.size
    assert_equal "https://sqs.us-east-1.amazonaws.com/123/queue", sqs.deletions.first[:queue_url]
  ensure
    thread&.kill
  end
end
