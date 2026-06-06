# frozen_string_literal: true

require_relative "base"

module SharedBroker
  module Adapters
    class InMemory < Base
      def initialize
        @storage = Hash.new { |h, k| h[k] = [] }
        @subscribers = Hash.new { |h, k| h[k] = [] }
      end

      def publish(topic, message)
        @storage[topic] << message
        @subscribers[topic].each { |callback| callback.call(message) }
      end

      def subscribe(topic, _queue_name, &block)
        @subscribers[topic] << block
      end

      def published_messages(topic)
        @storage[topic]
      end
    end
  end
end
