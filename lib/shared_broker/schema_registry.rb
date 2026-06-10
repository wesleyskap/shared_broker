# frozen_string_literal: true

module SharedBroker
  module SchemaRegistry
    class << self
      attr_accessor :provider
    end

    def self.validate!(topic, payload)
      resolved_provider = provider || default_provider
      resolved_provider.validate!(topic, payload)
    end

    def self.clear_cache
      return unless provider.respond_to?(:clear_cache)

      provider.clear_cache
    end

    def self.default_provider
      @default_provider ||= SharedBroker::SchemaRegistry::Providers::Local.new
    end
    private_class_method :default_provider
  end
end
