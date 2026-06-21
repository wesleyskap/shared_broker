# frozen_string_literal: true

require_relative "base"
require "json"
require "time"

module SharedBroker
  module Adapters
    class Redis < Base
      def initialize(redis_url:)
        begin
          require "redis"
        rescue LoadError
          raise unless defined?(::Redis)
        end
        @redis = ::Redis.new(url: redis_url)
      end

      def publish(topic, message, correlation_id: nil)
        unless message.is_a?(Hash)
          raise ArgumentError, "Expected message to be a Hash, got #{message.class} with value #{message.inspect}"
        end

        payload = message.merge(_correlation_id: correlation_id)
        @redis.publish(topic, payload.to_json)
      end

      def subscribe(topic, queue_name, max_retries: 3, backoff_base: 2, &block)
        Thread.new do
          @redis.subscribe(topic) do |on|
            on.message do |_channel, msg_json|
              data = JSON.parse(msg_json, symbolize_names: true)
              attempts = 0
              begin
                block.call(data)
              rescue SharedBroker::ShutdownError
                @redis.unsubscribe
              rescue => e
                attempts += 1
                if attempts <= max_retries
                  sleep(backoff_base**attempts)
                  retry
                else
                  publish_to_dlq(topic, queue_name, msg_json, e)
                end
              end
            end
          end
        end
      end

      def redrive_dlq(dlq_name, original_topic, limit: nil)
        count = 0
        loop do
          break if limit && count >= limit
          raw_json = @redis.lpop(dlq_name)
          break unless raw_json

          dlq_data = JSON.parse(raw_json, symbolize_names: true)
          original_payload = dlq_data[:payload]
          publish(original_topic, original_payload, correlation_id: original_payload[:_correlation_id])
          count += 1
        end
      end

      private

      def publish_to_dlq(topic, queue_name, payload_json, exception)
        dlq_key = "dlq:#{topic}:#{queue_name}"
        dlq_payload = {
          payload: JSON.parse(payload_json, symbolize_names: true),
          x_failed_at: Time.now.utc.iso8601,
          x_exception_class: exception.class.name,
          x_exception_message: exception.message
        }
        @redis.rpush(dlq_key, dlq_payload.to_json)
      end
    end
  end
end
