# frozen_string_literal: true

require_relative "shared_broker/version"
require_relative "shared_broker/telemetry"
require_relative "shared_broker/adapters/base"
require_relative "shared_broker/adapters/in_memory"
require_relative "shared_broker/adapters/rabbit_mq"

module SharedBroker
  class Client
    def initialize(adapter:)
      unless adapter.respond_to?(:publish) && adapter.respond_to?(:subscribe)
        raise ArgumentError, "Expected adapter to respond to :publish and :subscribe, got #{adapter.class} with value #{adapter.inspect}"
      end

      @adapter = adapter
    end

    def publish(topic, message)
      @adapter.publish(topic, message)
    end

    def subscribe(topic, queue_name, &block)
      @adapter.subscribe(topic, queue_name, &block)
    end
  end
end
