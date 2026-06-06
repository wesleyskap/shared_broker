# frozen_string_literal: true

require "openssl"
require "base64"
require "json"

module SharedBroker
  module Cipher
    class DecryptionError < StandardError; end

    ALGORITHM = "aes-256-gcm"

    def self.encrypt(payload_hash, key)
      return payload_hash unless key

      unless payload_hash.is_a?(Hash)
        raise ArgumentError, "Expected payload_hash to be a Hash, got #{payload_hash.class} with value #{payload_hash.inspect}"
      end

      # We don't want to encrypt correlation ID or special metadata keys if we want the broker to read them,
      # but we want to encrypt the actual payload data.
      # Let's extract metadata starting with underscore and encrypt the rest.
      metadata = payload_hash.select { |k, _| k.to_s.start_with?("_") }
      data_to_encrypt = payload_hash.reject { |k, _| k.to_s.start_with?("_") }

      cipher = OpenSSL::Cipher.new(ALGORITHM)
      cipher.encrypt
      cipher.key = key
      iv = cipher.random_iv

      encrypted_data = cipher.update(data_to_encrypt.to_json) + cipher.final
      auth_tag = cipher.auth_tag

      metadata.merge(
        _encrypted: true,
        _iv: Base64.strict_encode64(iv),
        _auth_tag: Base64.strict_encode64(auth_tag),
        _data: Base64.strict_encode64(encrypted_data)
      )
    end

    def self.decrypt(payload_hash, key)
      return payload_hash unless payload_hash.is_a?(Hash) && payload_hash[:_encrypted]
      return payload_hash unless key

      cipher = OpenSSL::Cipher.new(ALGORITHM)
      cipher.decrypt
      cipher.key = key
      cipher.iv = Base64.strict_decode64(payload_hash[:_iv])
      cipher.auth_tag = Base64.strict_decode64(payload_hash[:_auth_tag])

      encrypted_bytes = Base64.strict_decode64(payload_hash[:_data])
      decrypted_json = cipher.update(encrypted_bytes) + cipher.final

      decrypted_data = JSON.parse(decrypted_json, symbolize_names: true)
      
      # Merge back the unencrypted metadata
      metadata = payload_hash.select { |k, _| k.to_s.start_with?("_") }
      metadata.delete(:_encrypted)
      metadata.delete(:_iv)
      metadata.delete(:_auth_tag)
      metadata.delete(:_data)

      decrypted_data.merge(metadata)
    rescue => e
      raise DecryptionError, "Failed to decrypt payload. Error: #{e.message}. Offending payload: #{payload_hash.inspect}"
    end
  end
end
