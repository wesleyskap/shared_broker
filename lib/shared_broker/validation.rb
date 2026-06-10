# frozen_string_literal: true

module SharedBroker
  module Validation
    class ValidationError < StandardError; end

    def self.register(topic, schema)
      provider = SharedBroker::SchemaRegistry.provider || SharedBroker::SchemaRegistry.send(:default_provider)
      if provider.respond_to?(:register)
        provider.register(topic, schema)
      else
        raise RuntimeError, "Current SchemaRegistry provider #{provider.class} does not support local registration. Expected a provider that responds to :register."
      end
    end

    def self.validate!(topic, message)
      SharedBroker::SchemaRegistry.validate!(topic, message)
    end

    def self.clear
      SharedBroker::SchemaRegistry.clear_cache
      provider = SharedBroker::SchemaRegistry.provider || SharedBroker::SchemaRegistry.send(:default_provider)
      provider.clear if provider.respond_to?(:clear)
    end
  end
end

