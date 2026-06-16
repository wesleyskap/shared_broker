# frozen_string_literal: true

module SharedBroker
  module KeyProvider
    class KeyNotFoundError < StandardError; end

    # Static key provider for backward compatibility
    class Static
      def initialize(key)
        raise ArgumentError, "Expected key to be a 32-byte String, got #{key.inspect}" unless key.is_a?(String)
        @key = key
      end

      def key_for(_topic)
        @key
      end

      def key_for_id(_key_id)
        @key
      end

      def active_key_id_for(_topic)
        nil
      end
    end

    # Flexible registry for multiple keys and topic-based routing patterns
    class Registry
      def initialize(keys: {}, active_keys: {})
        validate_inputs!(keys, active_keys)
        @keys = keys.transform_keys(&:to_s)
        @active_keys = active_keys.transform_keys(&:to_s)
      end

      def key_for(topic)
        key_id = active_key_id_for(topic)
        key_for_id(key_id)
      end

      def key_for_id(key_id)
        return nil if key_id.nil?
        
        key = @keys[key_id.to_s]
        return key if key

        raise KeyNotFoundError, "Key ID #{key_id.inspect} not found in registered keys. Available keys: #{@keys.keys.inspect}"
      end

      def active_key_id_for(topic)
        topic_str = topic.to_s
        return @active_keys[topic_str] if @active_keys.key?(topic_str)

        pattern = find_matching_pattern(topic_str)
        return @active_keys[pattern] if pattern

        fallback_key_id
      end

      private

      def validate_inputs!(keys, active_keys)
        unless keys.is_a?(Hash) && active_keys.is_a?(Hash)
          raise ArgumentError, "Expected keys and active_keys to be Hashes, got keys: #{keys.inspect}, active_keys: #{active_keys.inspect}"
        end
      end

      def find_matching_pattern(topic_str)
        @active_keys.keys.find do |pattern|
          pattern != "*" && File.fnmatch?(pattern, topic_str)
        end
      end

      def fallback_key_id
        @active_keys["*"] || raise(KeyNotFoundError, "No active key configuration found for fallback '*' pattern")
      end
    end
  end
end
