# frozen_string_literal: true

require "opentelemetry"

module SharedBroker
  module Middlewares
    class OpenTelemetryPropagation
      def call(topic, message, metadata)
        if metadata[:operation] == :publish
          carrier = {}
          ::OpenTelemetry.propagation.inject(carrier)
          carrier.each do |key, value|
            message["_#{key}".to_sym] = value
          end
          yield
        elsif metadata[:operation] == :subscribe
          carrier = {}
          message.each do |key, value|
            if key.to_s.start_with?("_trace")
              carrier[key.to_s.sub(/^_/, "")] = value
            end
          end

          parent_context = ::OpenTelemetry.propagation.extract(carrier)
          ::OpenTelemetry::Context.with_current(parent_context) do
            tracer = ::OpenTelemetry.tracer_provider.tracer("shared_broker")
            tracer.in_span("#{topic} process", kind: :consumer) do |span|
              span.set_attribute("messaging.system", "shared_broker")
              span.set_attribute("messaging.destination", topic)
              yield
            end
          end
        else
          yield
        end
      end
    end
  end
end
