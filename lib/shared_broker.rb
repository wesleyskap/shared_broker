# frozen_string_literal: true

require_relative "shared_broker/version"
require_relative "shared_broker/telemetry"
require_relative "shared_broker/circuit_breaker"
require_relative "shared_broker/schema_registry"
require_relative "shared_broker/schema_registry/providers/local"
require_relative "shared_broker/schema_registry/providers/http"
require_relative "shared_broker/validation"
require_relative "shared_broker/cipher"
require_relative "shared_broker/key_provider"
require_relative "shared_broker/compressor"
require_relative "shared_broker/concurrency/semaphore"
require_relative "shared_broker/concurrency/limiter"
require_relative "shared_broker/middleware_pipeline"
require_relative "shared_broker/middlewares/open_telemetry_propagation"
require_relative "shared_broker/middlewares/idempotency"
require_relative "shared_broker/dlq/redriver"
require_relative "shared_broker/adapters/base"
require_relative "shared_broker/adapters/in_memory"
require_relative "shared_broker/adapters/rabbit_mq"
require_relative "shared_broker/adapters/kafka"
require_relative "shared_broker/adapters/redis"

module SharedBroker
  ShutdownError = Class.new(StandardError)

  class << self
    attr_accessor :encryption_key, :key_provider, :compression_algorithm, :compression_threshold, :cache_store, :shutdown_requested, :registered_clients
  end

  # Default key for development/test if not set
  @encryption_key = ENV.fetch("SHARED_BROKER_ENCRYPTION_KEY") { "a" * 32 }
  @key_provider = nil
  @compression_algorithm = nil
  @compression_threshold = 1024
  @cache_store = nil
  @shutdown_requested = false
  @registered_clients = []
  @registered_clients_mutex = Mutex.new

  def self.register_client(client)
    @registered_clients_mutex.synchronize do
      @registered_clients << client
    end
  end

  def self.shutdown!(timeout: 10)
    @shutdown_requested = true
    
    threads = @registered_clients_mutex.synchronize do
      @registered_clients.flat_map(&:active_threads)
    end

    threads.each { |t| t.join(timeout) }
  end

  def self.reset_shutdown!
    @shutdown_requested = false
  end

  class Client
    attr_reader :circuit_breaker, :middleware_pipeline, :adapters, :routing

    def initialize(adapter: nil, adapters: nil, routing: nil, circuit_breaker: nil, middlewares: nil, key_provider: nil)
      setup_adapters(adapter: adapter, adapters: adapters, routing: routing)
      @circuit_breaker = circuit_breaker || CircuitBreaker.new
      resolved_middlewares = middlewares || [SharedBroker::Middlewares::OpenTelemetryPropagation.new]
      @middleware_pipeline = MiddlewarePipeline.new(resolved_middlewares)
      @key_provider = key_provider
      @running_threads = []
      @running_threads_mutex = Mutex.new
      SharedBroker.register_client(self)
    end

    def register_thread(thread)
      @running_threads_mutex.synchronize do
        @running_threads << thread
      end
    end

    def active_threads
      @running_threads_mutex.synchronize do
        @running_threads.select(&:alive?)
      end
    end

    def publish(topic, message, correlation_id: nil)
      metadata = { correlation_id: correlation_id, operation: :publish }
      @middleware_pipeline.execute(topic, message, metadata) do
        SharedBroker::Validation.validate!(topic, message)
        encrypted_msg = SharedBroker::Cipher.encrypt(message, active_key_provider, topic: topic)

        @circuit_breaker.run do
          resolve_adapter(topic).publish(topic, encrypted_msg, correlation_id: correlation_id)
        end
      end
    end

    def publish_batch(topic, messages, correlation_id: nil)
      unless messages.is_a?(Array)
        raise ArgumentError, "Expected messages to be an Array, got #{messages.class} with value #{messages.inspect}"
      end

      processed_messages = messages.map do |message|
        SharedBroker::Validation.validate!(topic, message)
        SharedBroker::Cipher.encrypt(message, active_key_provider, topic: topic)
      end

      metadata = { correlation_id: correlation_id, operation: :publish_batch }
      @middleware_pipeline.execute(topic, processed_messages, metadata) do
        @circuit_breaker.run do
          resolve_adapter(topic).publish_batch(topic, processed_messages, correlation_id: correlation_id)
        end
      end
    end

    def subscribe(topic, queue_name, max_retries: 3, backoff_base: 2, max_concurrency: nil, backpressure_check: nil, backpressure_backoff: 1.0, &block)
      limiter = SharedBroker::Concurrency::Limiter.new(
        max_concurrency: max_concurrency,
        backpressure_check: backpressure_check,
        backpressure_backoff: backpressure_backoff
      )

      res = resolve_adapter(topic).subscribe(topic, queue_name, max_retries: max_retries, backoff_base: backoff_base) do |raw_message|
        raise SharedBroker::ShutdownError, "Shutdown requested" if SharedBroker.shutdown_requested

        limiter.run do
          raise SharedBroker::ShutdownError, "Shutdown requested" if SharedBroker.shutdown_requested

          decrypted_msg = SharedBroker::Cipher.decrypt(raw_message, active_key_provider, topic: topic)
          SharedBroker::Validation.validate!(topic, decrypted_msg)

          metadata = { correlation_id: decrypted_msg[:_correlation_id], operation: :subscribe, queue_name: queue_name }
          @middleware_pipeline.execute(topic, decrypted_msg, metadata) do
            block.call(decrypted_msg)
          end
        end
      end

      register_thread(res) if res.is_a?(Thread)
      res
    end

    private

    def active_key_provider
      @key_provider || SharedBroker.key_provider || SharedBroker::KeyProvider::Static.new(SharedBroker.encryption_key)
    end

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
