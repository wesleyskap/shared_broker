# frozen_string_literal: true

require "time"
require_relative "base"

module SharedBroker
  module Adapters
    class InMemory < Base
      def initialize
        @storage = Hash.new { |h, k| h[k] = [] }
        @subscribers = Hash.new { |h, k| h[k] = [] }
      end

      def publish(topic, message, correlation_id: nil)
        msg_with_metadata = message.merge(_correlation_id: correlation_id)
        @storage[topic] << msg_with_metadata
        
        @subscribers[topic].each do |sub|
          attempts = 0
          begin
            sub[:block].call(msg_with_metadata)
          rescue => e
            attempts += 1
            if attempts <= sub[:max_retries]
              # Sleep briefly or not at all in memory to keep tests fast
              sleep(0.001 * sub[:backoff_base]**attempts)
              retry
            else
              dlq_topic = "#{sub[:queue_name]}.dlq"
              dlq_msg = msg_with_metadata.merge(
                _x_original_routing_key: topic,
                _x_failed_at: Time.now.utc.iso8601,
                _x_exception_class: e.class.name,
                _x_exception_message: e.message
              )
              @storage[dlq_topic] << dlq_msg
            end
          end
        end
      end

      def subscribe(topic, queue_name, max_retries: 3, backoff_base: 2, &block)
        @subscribers[topic] << { queue_name: queue_name, max_retries: max_retries, backoff_base: backoff_base, block: block }
      end

      def published_messages(topic)
        @storage[topic]
      end

      def redrive_dlq(dlq_name, original_topic, limit: nil)
        dlq_messages = @storage[dlq_name]
        return if dlq_messages.empty?

        to_redrive = limit ? dlq_messages.first(limit) : dlq_messages.dup
        to_redrive.each do |msg|
          cleaned_msg = msg.dup
          cleaned_msg.delete(:_x_original_routing_key)
          cleaned_msg.delete(:_x_failed_at)
          cleaned_msg.delete(:_x_exception_class)
          cleaned_msg.delete(:_x_exception_message)

          publish(original_topic, cleaned_msg, correlation_id: cleaned_msg[:_correlation_id])
          @storage[dlq_name].delete(msg)
        end
      end
    end
  end
end
