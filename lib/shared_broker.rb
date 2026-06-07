# frozen_string_literal: true

require_relative "shared_broker/version"
require_relative "shared_broker/telemetry"
require_relative "shared_broker/circuit_breaker"
require_relative "shared_broker/validation"
require_relative "shared_broker/cipher"
require_relative "shared_broker/middleware_pipeline"
require_relative "shared_broker/middlewares/open_telemetry_propagation"
require_relative "shared_broker/adapters/base"
require_relative "shared_broker/adapters/in_memory"
require_relative "shared_broker/adapters/rabbit_mq"
require_relative "shared_broker/adapters/kafka"
require_relative "shared_broker/adapters/redis"

module SharedBroker
  class << self
    attr_accessor :encryption_key
  end

  # Default key for development/test if not set
  @encryption_key = ENV.fetch("SHARED_BROKER_ENCRYPTION_KEY") { "a" * 32 }

  class Client
    attr_reader :circuit_breaker, :middleware_pipeline

    def initialize(adapter:, circuit_breaker: nil, middlewares: nil)
      unless adapter.respond_to?(:publish) && adapter.respond_to?(:subscribe)
        raise ArgumentError, "Expected adapter to respond to :publish and :subscribe, got #{adapter.class} with value #{adapter.inspect}"
      end

      @adapter = adapter
      @circuit_breaker = circuit_breaker || CircuitBreaker.new
      
      resolved_middlewares = middlewares || [SharedBroker::Middlewares::OpenTelemetryPropagation.new]
      @middleware_pipeline = MiddlewarePipeline.new(resolved_middlewares)
    end

    def publish(topic, message, correlation_id: nil)
      metadata = { correlation_id: correlation_id, operation: :publish }
      @middleware_pipeline.execute(topic, message, metadata) do
        SharedBroker::Validation.validate!(topic, message)
        encrypted_msg = SharedBroker::Cipher.encrypt(message, SharedBroker.encryption_key)

        @circuit_breaker.run do
          @adapter.publish(topic, encrypted_msg, correlation_id: correlation_id)
        end
      end
    end

    def subscribe(topic, queue_name, max_retries: 3, backoff_base: 2, &block)
      @adapter.subscribe(topic, queue_name, max_retries: max_retries, backoff_base: backoff_base) do |raw_message|
        decrypted_msg = SharedBroker::Cipher.decrypt(raw_message, SharedBroker.encryption_key)
        SharedBroker::Validation.validate!(topic, decrypted_msg)

        metadata = { correlation_id: decrypted_msg[:_correlation_id], operation: :subscribe, queue_name: queue_name }
        @middleware_pipeline.execute(topic, decrypted_msg, metadata) do
          block.call(decrypted_msg)
        end
      end
    end
  end
end
