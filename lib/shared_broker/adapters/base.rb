# frozen_string_literal: true

module SharedBroker
  module Adapters
    class Base
      def publish(topic, message, correlation_id: nil)
        raise NotImplementedError, "#{self.class.name} must implement #publish"
      end

      def subscribe(topic, queue_name, max_retries: 3, backoff_base: 2, &block)
        raise NotImplementedError, "#{self.class.name} must implement #subscribe"
      end
    end
  end
end
