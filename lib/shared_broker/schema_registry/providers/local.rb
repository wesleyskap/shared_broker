# frozen_string_literal: true

require "dry-schema"

module SharedBroker
  module SchemaRegistry
    module Providers
      class Local
        def initialize
          @schemas = {}
        end

        def register(topic, schema)
          unless schema.respond_to?(:call)
            raise ArgumentError, "Expected schema to respond to :call, got #{schema.class} with value #{schema.inspect}. Expected shape: respond_to?(:call)"
          end

          @schemas[topic.to_s] = schema
        end

        def validate!(topic, payload)
          schema = @schemas[topic.to_s]
          return unless schema

          result = schema.call(payload)
          return if result.success?

          raise SharedBroker::Validation::ValidationError,
                "Schema validation failed for topic #{topic.inspect}. Expected keys: #{schema.rules.keys.inspect}, got payload: #{payload.inspect}. Errors: #{result.errors.to_h.inspect}"
        end

        def clear
          @schemas.clear
        end
      end
    end
  end
end
