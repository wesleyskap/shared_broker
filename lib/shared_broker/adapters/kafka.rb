# frozen_string_literal: true

require_relative "base"
require "json"
require "time"

module SharedBroker
  module Adapters
    class Kafka < Base
      def initialize(seed_brokers:, client_id: "shared_broker")
        require "kafka"
        @kafka = ::Kafka.new(seed_brokers, client_id: client_id)
      end

      def publish(topic, message, correlation_id: nil)
        unless message.is_a?(Hash)
          raise ArgumentError, "Expected message to be a Hash, got #{message.class} with value #{message.inspect}"
        end

        headers = {}
        headers["correlation_id"] = correlation_id if correlation_id
        
        @kafka.deliver_message(message.to_json, topic: topic, headers: headers)
      end

      def subscribe(topic, queue_name, max_retries: 3, backoff_base: 2, &block)
        consumer = @kafka.consumer(group_id: queue_name)
        consumer.subscribe(topic)
        
        Thread.new do
          consumer.each_message do |message|
            data = JSON.parse(message.value, symbolize_names: true)
            if message.headers && message.headers["correlation_id"]
              data[:_correlation_id] = message.headers["correlation_id"]
            end
            
            attempts = 0
            begin
              block.call(data)
            rescue => e
              attempts += 1
              if attempts <= max_retries
                sleep(backoff_base**attempts)
                retry
              else
                publish_to_dlq(topic, queue_name, message.value, message.headers, e)
              end
            end
          end
        end
      end

      private

      def publish_to_dlq(original_topic, queue_name, payload, original_headers, exception)
        dlq_topic = "#{original_topic}.#{queue_name}.dlq"
        headers = (original_headers || {}).merge(
          "x_original_topic" => original_topic,
          "x_failed_at" => Time.now.utc.iso8601,
          "x_exception_class" => exception.class.name,
          "x_exception_message" => exception.message
        )
        @kafka.deliver_message(payload, topic: dlq_topic, headers: headers)
      end
    end
  end
end
