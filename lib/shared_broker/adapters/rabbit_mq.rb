# frozen_string_literal: true

require "bunny"
require "json"
require "time"
require_relative "base"

module SharedBroker
  module Adapters
    class RabbitMQ < Base
      EXCHANGE_NAME = "shared_broker_events"

      def initialize(amqp_url:)
        @connection = Bunny.new(amqp_url)
        @connection.start
        @channel = @connection.create_channel
        @exchange = @channel.topic(EXCHANGE_NAME, durable: true)
      end

      def publish(topic, message, correlation_id: nil)
        unless message.is_a?(Hash)
          raise ArgumentError, "Message must be a Hash, got #{message.class} with value #{message.inspect}"
        end

        options = { routing_key: topic }
        options[:correlation_id] = correlation_id if correlation_id
        @exchange.publish(message.to_json, options)
      end

      def subscribe(topic, queue_name, max_retries: 3, backoff_base: 2, &block)
        queue = @channel.queue(queue_name, durable: true)
        queue.bind(@exchange, routing_key: topic)
        
        queue.subscribe(manual_ack: true) do |delivery_info, metadata, payload|
          data = JSON.parse(payload, symbolize_names: true)
          if metadata.respond_to?(:correlation_id) && metadata.correlation_id
            data[:_correlation_id] = metadata.correlation_id
          end

          attempts = 0
          begin
            block.call(data)
            @channel.acknowledge(delivery_info.delivery_tag, false)
          rescue => e
            attempts += 1
            if attempts <= max_retries
              sleep(backoff_base**attempts)
              retry
            else
              publish_to_dlq(queue_name, payload, delivery_info, metadata, e)
              @channel.acknowledge(delivery_info.delivery_tag, false)
            end
          end
        end
      end

      def close
        @channel.close if @channel
        @connection.close if @connection
      end

      private

      def publish_to_dlq(queue_name, payload, delivery_info, metadata, exception)
        dlq_name = "#{queue_name}.dlq"
        dlq_queue = @channel.queue(dlq_name, durable: true)
        
        headers = {
          x_original_routing_key: delivery_info.routing_key,
          x_failed_at: Time.now.utc.iso8601,
          x_exception_class: exception.class.name,
          x_exception_message: exception.message
        }
        
        @channel.default_exchange.publish(
          payload,
          routing_key: dlq_queue.name,
          correlation_id: metadata.correlation_id,
          headers: headers
        )
      end
    end
  end
end
