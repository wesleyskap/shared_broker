# frozen_string_literal: true

require_relative "shared_broker/version"
require_relative "shared_broker/telemetry"
require_relative "shared_broker/circuit_breaker"
require_relative "shared_broker/schema_registry"
require_relative "shared_broker/schema_registry/providers/local"
require_relative "shared_broker/schema_registry/providers/http"
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
    attr_reader :circuit_breaker, :middleware_pipeline, :adapters, :routing

    def initialize(adapter: nil, adapters: nil, routing: nil, circuit_breaker: nil, middlewares: nil)
      setup_adapters(adapter: adapter, adapters: adapters, routing: routing)
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
          resolve_adapter(topic).publish(topic, encrypted_msg, correlation_id: correlation_id)
        end
      end
    end

    def subscribe(topic, queue_name, max_retries: 3, backoff_base: 2, &block)
      resolve_adapter(topic).subscribe(topic, queue_name, max_retries: max_retries, backoff_base: backoff_base) do |raw_message|
        decrypted_msg = SharedBroker::Cipher.decrypt(raw_message, SharedBroker.encryption_key)
        SharedBroker::Validation.validate!(topic, decrypted_msg)

        metadata = { correlation_id: decrypted_msg[:_correlation_id], operation: :subscribe, queue_name: queue_name }
        @middleware_pipeline.execute(topic, decrypted_msg, metadata) do
          block.call(decrypted_msg)
        end
      end
    end

    private

    def setup_adapters(adapter: nil, adapters: nil, routing: nil)
      if adapter || (adapters.nil? && routing.nil?)
        validate_single_adapter!(adapter)
        @adapters = { default: adapter }
        @routing = { "*" => :default }
      else
        validate_multi_adapters!(adapters, routing)
        @adapters = adapters
        @routing = routing.transform_keys(&:to_s)
      end
    end

    def validate_single_adapter!(adapter)
      unless adapter.respond_to?(:publish) && adapter.respond_to?(:subscribe)
        raise ArgumentError, "Expected adapter to respond to :publish and :subscribe, got: #{adapter.inspect} (must shape like SharedBroker::Adapters::Base)"
      end
    end

    def validate_multi_adapters!(adapters, routing)
      unless adapters.is_a?(Hash) && routing.is_a?(Hash)
        raise ArgumentError, "Expected adapters and routing to be Hashes, got adapters: #{adapters.inspect} (class: #{adapters.class}), routing: #{routing.inspect} (class: #{routing.class})"
      end
      adapters.each do |key, ad|
        unless ad.respond_to?(:publish) && ad.respond_to?(:subscribe)
          raise ArgumentError, "Expected adapter #{key.inspect} to respond to :publish and :subscribe, got: #{ad.inspect} (class: #{ad.class})"
        end
      end
    end

    def resolve_adapter(topic)
      topic_str = topic.to_s
      return @adapters[@routing[topic_str]] if @routing.key?(topic_str)

      @routing.each do |pattern, adapter_key|
        next if pattern == "*"
        if File.fnmatch?(pattern, topic_str)
          return @adapters[adapter_key]
        end
      end

      fallback_key = @routing["*"]
      if fallback_key && @adapters.key?(fallback_key)
        @adapters[fallback_key]
      else
        raise RuntimeError, "No adapter resolved for topic: #{topic.inspect}. Expected one of #{@routing.keys.inspect}"
      end
    end
  end
end
