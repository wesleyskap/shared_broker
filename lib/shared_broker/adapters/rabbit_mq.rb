# frozen_string_literal: true

require "bunny"
require "json"
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

      def publish(topic, message)
        unless message.is_a?(Hash)
          raise ArgumentError, "Message must be a Hash, got #{message.class} with value #{message.inspect}"
        end

        @exchange.publish(message.to_json, routing_key: topic)
      end

      def subscribe(topic, queue_name, &block)
        queue = @channel.queue(queue_name, durable: true)
        queue.bind(@exchange, routing_key: topic)
        queue.subscribe(manual_ack: false) do |_delivery_info, _metadata, payload|
          data = JSON.parse(payload, symbolize_names: true)
          block.call(data)
        end
      end

      def close
        @channel.close if @channel
        @connection.close if @connection
      end
    end
  end
end
