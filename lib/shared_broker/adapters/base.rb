# frozen_string_literal: true

module SharedBroker
  module Adapters
    class Base
      def publish(topic, message, correlation_id: nil)
        raise NotImplementedError, "#{self.class.name} must implement #publish"
      end

      def publish_batch(topic, messages, correlation_id: nil)
        unless messages.is_a?(Array)
          raise ArgumentError, "Expected messages to be an Array, got #{messages.class} with value #{messages.inspect}"
        end

        messages.each do |message|
          publish(topic, message, correlation_id: correlation_id)
        end
      end

      def subscribe(topic, queue_name, max_retries: 3, backoff_base: 2, &block)
        raise NotImplementedError, "#{self.class.name} must implement #subscribe"
      end

      def redrive_dlq(dlq_name, original_topic, limit: nil)
        raise NotImplementedError, "#{self.class.name} must implement #redrive_dlq"
      end
    end
  end
end
