# frozen_string_literal: true

require "openssl"
require "base64"
require "json"

module SharedBroker
  module Cipher
    class DecryptionError < StandardError; end

    ALGORITHM = "aes-256-gcm"

    def self.encrypt(payload_hash, key_provider_or_key, topic: nil)
      return payload_hash unless key_provider_or_key

      unless payload_hash.is_a?(Hash)
        raise ArgumentError, "Expected payload_hash to be a Hash, got #{payload_hash.class} with value #{payload_hash.inspect}"
      end

      key, key_id = resolve_encryption_key(key_provider_or_key, topic)
      return payload_hash unless key

      metadata = payload_hash.select { |k, _| k.to_s.start_with?("_") }
      data_to_encrypt = payload_hash.reject { |k, _| k.to_s.start_with?("_") }

      cipher = OpenSSL::Cipher.new(ALGORITHM)
      cipher.encrypt
      cipher.key = key
      iv = cipher.random_iv

      encrypted_data = cipher.update(data_to_encrypt.to_json) + cipher.final
      auth_tag = cipher.auth_tag

      envelope = {
        _encrypted: true,
        _iv: Base64.strict_encode64(iv),
        _auth_tag: Base64.strict_encode64(auth_tag),
        _data: Base64.strict_encode64(encrypted_data)
      }
      envelope[:_key_id] = key_id.to_s if key_id

      metadata.merge(envelope)
    end

    def self.decrypt(payload_hash, key_provider_or_key, topic: nil)
      return payload_hash unless payload_hash.is_a?(Hash) && payload_hash[:_encrypted]
      return payload_hash unless key_provider_or_key

      key = resolve_decryption_key(payload_hash, key_provider_or_key, topic)
      return payload_hash unless key

      cipher = OpenSSL::Cipher.new(ALGORITHM)
      cipher.decrypt
      cipher.key = key
      cipher.iv = Base64.strict_decode64(payload_hash[:_iv])
      cipher.auth_tag = Base64.strict_decode64(payload_hash[:_auth_tag])

      encrypted_bytes = Base64.strict_decode64(payload_hash[:_data])
      decrypted_json = cipher.update(encrypted_bytes) + cipher.final

      decrypted_data = JSON.parse(decrypted_json, symbolize_names: true)
      
      metadata = payload_hash.select { |k, _| k.to_s.start_with?("_") }
      clean_envelope_metadata!(metadata)

      decrypted_data.merge(metadata)
    rescue => e
      raise DecryptionError, "Failed to decrypt payload. Error: #{e.message}. Offending payload: #{payload_hash.inspect}"
    end

    private

    def self.resolve_encryption_key(provider_or_key, topic)
      if provider_or_key.respond_to?(:key_for)
        [provider_or_key.key_for(topic), provider_or_key.active_key_id_for(topic)]
      else
        [provider_or_key, nil]
      end
    end

    def self.resolve_decryption_key(payload, provider_or_key, topic)
      if provider_or_key.respond_to?(:key_for_id)
        key_id = payload[:_key_id]
        key_id ? provider_or_key.key_for_id(key_id) : provider_or_key.key_for(topic)
      else
        provider_or_key
      end
    end

    def self.clean_envelope_metadata!(metadata)
      metadata.delete(:_encrypted)
      metadata.delete(:_iv)
      metadata.delete(:_auth_tag)
      metadata.delete(:_data)
      metadata.delete(:_key_id)
    end
  end
end
