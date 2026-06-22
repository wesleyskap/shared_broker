# frozen_string_literal: true

require_relative "base"
require "json"
require "time"

module SharedBroker
  module Adapters
    class AwsSqsSns < Base
      def initialize(sqs_client: nil, sns_client: nil)
        @sqs_client = sqs_client
        @sns_client = sns_client
      end

      def publish(topic, message, correlation_id: nil)
        unless message.is_a?(Hash)
          raise ArgumentError, "Expected message to be a Hash, got #{message.class} with value #{message.inspect}"
        end

        payload = message.merge(_correlation_id: correlation_id)
        sns.publish(topic_arn: topic, message: payload.to_json)
      end

      def subscribe(topic, queue_name, max_retries: 3, backoff_base: 2, &block)
        Thread.new do
          loop do
            break if SharedBroker.shutdown_requested
            poll_messages(topic, queue_name, max_retries, backoff_base, &block)
          end
        end
      end

      def redrive_dlq(dlq_name, original_topic, limit: nil)
        count = 0
        loop do
          break if limit && count >= limit
          msg = sqs.receive_message(queue_url: dlq_name, max_number_of_messages: 1).messages.first
          break unless msg

          data = JSON.parse(msg.body, symbolize_names: true)
          publish(original_topic, data, correlation_id: data[:_correlation_id])
          sqs.delete_message(queue_url: dlq_name, receipt_handle: msg.receipt_handle)
          count += 1
        end
      end

      private

      def sqs
        @sqs_client || (require "aws-sdk-sqs"; Aws::SQS::Client.new)
      end

      def sns
        @sns_client || (require "aws-sdk-sns"; Aws::SNS::Client.new)
      end

      def poll_messages(topic, queue_name, max_retries, backoff_base, &block)
        response = sqs.receive_message(queue_url: queue_name, max_number_of_messages: 10, wait_time_seconds: 2)
        return unless response&.messages

        response.messages.each do |message|
          process_message(message, topic, queue_name, max_retries, backoff_base, &block)
        end
      end

      def process_message(message, topic, queue_name, max_retries, backoff_base, &block)
        data = JSON.parse(message.body, symbolize_names: true)
        attempts = 0
        begin
          block.call(data)
          sqs.delete_message(queue_url: queue_name, receipt_handle: message.receipt_handle)
        rescue SharedBroker::ShutdownError => e
          raise e
        rescue => e
          attempts += 1
          if attempts <= max_retries
            sleep(backoff_base**attempts)
            retry
          else
            publish_to_dlq(queue_name, message.body, e)
            sqs.delete_message(queue_url: queue_name, receipt_handle: message.receipt_handle)
          end
        end
      end

      def publish_to_dlq(queue_name, body, exception)
        dlq_url = "#{queue_name}.dlq"
        dlq_payload = {
          payload: JSON.parse(body, symbolize_names: true),
          x_failed_at: Time.now.utc.iso8601,
          x_exception_class: exception.class.name,
          x_exception_message: exception.message
        }
        sqs.send_message(queue_url: dlq_url, message_body: dlq_payload.to_json)
      end
    end
  end
end
