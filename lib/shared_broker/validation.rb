# frozen_string_literal: true

require "dry-schema"

module SharedBroker
  module Validation
    class ValidationError < StandardError; end

    @schemas = {}

    def self.register(topic, schema)
      unless schema.respond_to?(:call)
        raise ArgumentError, "Expected schema to respond to :call, got #{schema.class} with value #{schema.inspect}"
      end
      @schemas[topic.to_s] = schema
    end

    def self.validate!(topic, message)
      schema = @schemas[topic.to_s]
      return unless schema

      result = schema.call(message)
      unless result.success?
        raise ValidationError, "Schema validation failed for topic #{topic.inspect}. Expected keys: #{schema.rules.keys.inspect}, got payload: #{message.inspect}. Errors: #{result.errors.to_h.inspect}"
      end
    end

    def self.clear
      @schemas.clear
    end
  end
end
