# frozen_string_literal: true

module SharedBroker
  module Middlewares
    class Idempotency
      class MemoryStore
        def initialize
          @store = {}
        end

        def exists?(key)
          prune
          @store.key?(key)
        end

        def write(key, value, expires_in: 3600)
          @store[key] = Time.now + expires_in
        end

        def prune
          now = Time.now
          @store.delete_if { |_, expiry| expiry < now }
        end
      end

      def initialize(store: nil, expires_in: 3600)
        @store = store || MemoryStore.new
        @expires_in = expires_in
      end

      def call(topic, message, metadata)
        return yield unless metadata[:operation] == :subscribe

        correlation_id = metadata[:correlation_id] || message[:_correlation_id]
        return yield unless correlation_id

        key = "shared_broker:idempotency:#{topic}:#{correlation_id}"
        return if duplicate?(key)

        mark_processed(key)
        yield
      end

      private

      def duplicate?(key)
        if @store.respond_to?(:exist?)
          @store.exist?(key)
        elsif @store.respond_to?(:exists?)
          @store.exists?(key)
        else
          false
        end
      end

      def mark_processed(key)
        @store.write(key, true, expires_in: @expires_in)
      rescue ArgumentError
        @store.write(key, @expires_in)
      end
    end
  end
end
