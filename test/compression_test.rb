# frozen_string_literal: true

require "test_helper"

class CompressionTest < Minitest::Test
  def setup
    @key = "a" * 32
    SharedBroker.encryption_key = @key
    SharedBroker.compression_algorithm = nil
    SharedBroker.compression_threshold = 10
  end

  def teardown
    SharedBroker.compression_algorithm = nil
    SharedBroker.compression_threshold = 1024
  end

  def test_no_compression_by_default
    payload = { text: "Some normal payload data" }
    encrypted = SharedBroker::Cipher.encrypt(payload, @key)
    
    assert encrypted[:_encrypted]
    refute encrypted.key?(:_compression)

    decrypted = SharedBroker::Cipher.decrypt(encrypted, @key)
    assert_equal "Some normal payload data", decrypted[:text]
  end

  def test_no_compression_below_threshold
    SharedBroker.compression_algorithm = :gzip
    SharedBroker.compression_threshold = 1000

    payload = { text: "Short text" }
    encrypted = SharedBroker::Cipher.encrypt(payload, @key)

    refute encrypted.key?(:_compression)

    decrypted = SharedBroker::Cipher.decrypt(encrypted, @key)
    assert_equal "Short text", decrypted[:text]
  end

  def test_gzip_compression_above_threshold
    SharedBroker.compression_algorithm = :gzip
    SharedBroker.compression_threshold = 5

    payload = { text: "This is a longer text that is going to be compressed because it exceeds the threshold limit" }
    encrypted = SharedBroker::Cipher.encrypt(payload, @key)

    assert_equal "gzip", encrypted[:_compression]

    decrypted = SharedBroker::Cipher.decrypt(encrypted, @key)
    assert_equal payload[:text], decrypted[:text]
  end

  def test_deflate_compression_above_threshold
    SharedBroker.compression_algorithm = :deflate
    SharedBroker.compression_threshold = 5

    payload = { text: "This is a longer text that is going to be compressed because it exceeds the threshold limit" }
    encrypted = SharedBroker::Cipher.encrypt(payload, @key)

    assert_equal "deflate", encrypted[:_compression]

    decrypted = SharedBroker::Cipher.decrypt(encrypted, @key)
    assert_equal payload[:text], decrypted[:text]
  end

  def test_invalid_decompression_raises_error
    SharedBroker.compression_algorithm = :gzip
    SharedBroker.compression_threshold = 5

    payload = { text: "Valid long text to trigger compression" }
    encrypted = SharedBroker::Cipher.encrypt(payload, @key)

    encrypted[:_data] = Base64.strict_encode64("corrupted data")

    assert_raises(SharedBroker::Cipher::DecryptionError) do
      SharedBroker::Cipher.decrypt(encrypted, @key)
    end
  end

  def test_client_integration_with_compression
    SharedBroker.compression_algorithm = :gzip
    SharedBroker.compression_threshold = 10

    in_memory = SharedBroker::Adapters::InMemory.new
    client = SharedBroker::Client.new(adapter: in_memory)

    received = []
    client.subscribe("compression.topic", "queue") do |msg|
      received << msg
    end

    large_payload = { data: "x" * 200 }
    client.publish("compression.topic", large_payload)

    raw_msg = in_memory.published_messages("compression.topic").first
    assert_equal "gzip", raw_msg[:_compression]
    assert_equal "x" * 200, received.first[:data]
  end
end
