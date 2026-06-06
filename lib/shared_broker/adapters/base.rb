# frozen_string_literal: true

module SharedBroker
  module Adapters
    class Base
      def publish(topic, message)
        raise NotImplementedError, "#{self.class.name} must implement #publish"
      end

      def subscribe(topic, queue_name, &block)
        raise NotImplementedError, "#{self.class.name} must implement #subscribe"
      end
    end
  end
end
