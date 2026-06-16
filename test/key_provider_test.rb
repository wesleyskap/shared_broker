# frozen_string_literal: true

require "test_helper"

class KeyProviderTest < Minitest::Test
  def setup
    @key_v1 = "a" * 32
    @key_v2 = "b" * 32
    @finance_key = "c" * 32

    @registry = SharedBroker::KeyProvider::Registry.new(
      keys: {
        "v1" => @key_v1,
        "v2" => @key_v2,
        "finance" => @finance_key
      },
      active_keys: {
        "finance.*" => "finance",
        "*" => "v2"
      }
    )
  end

  def test_static_key_provider
    provider = SharedBroker::KeyProvider::Static.new("x" * 32)
    assert_equal "x" * 32, provider.key_for("any.topic")
    assert_equal "x" * 32, provider.key_for_id("any_id")
    assert_nil provider.active_key_id_for("any.topic")
  end

  def test_registry_resolves_keys_by_topic_pattern
    assert_equal @finance_key, @registry.key_for("finance.payments")
    assert_equal "finance", @registry.active_key_id_for("finance.payments")

    assert_equal @key_v2, @registry.key_for("other.events")
    assert_equal "v2", @registry.active_key_id_for("other.events")
  end

  def test_registry_resolves_keys_by_id
    assert_equal @key_v1, @registry.key_for_id("v1")
    assert_equal @key_v2, @registry.key_for_id("v2")
  end

  def test_registry_raises_on_missing_key_id
    assert_raises(SharedBroker::KeyProvider::KeyNotFoundError) do
      @registry.key_for_id("missing")
    end
  end

  def test_cipher_encrypt_decrypt_with_key_registry
    payload = { sensitive: "data", _correlation_id: "xyz" }
    
    # Encrypts using the active key for finance.payments ("finance")
    encrypted = SharedBroker::Cipher.encrypt(payload, @registry, topic: "finance.payments")
    assert encrypted[:_encrypted]
    assert_equal "finance", encrypted[:_key_id]

    # Decrypts using the key_id stored in the payload
    decrypted = SharedBroker::Cipher.decrypt(encrypted, @registry, topic: "finance.payments")
    assert_equal "data", decrypted[:sensitive]
    assert_equal "xyz", decrypted[:_correlation_id]
  end

  def test_key_rotation_decrypted_with_past_key
    payload = { sensitive: "old data" }
    
    # Manually encrypt with v1
    v1_provider = SharedBroker::KeyProvider::Registry.new(
      keys: { "v1" => @key_v1 },
      active_keys: { "*" => "v1" }
    )
    encrypted = SharedBroker::Cipher.encrypt(payload, v1_provider, topic: "some.topic")
    assert_equal "v1", encrypted[:_key_id]

    # Decrypt with registry that has v2 as active but v1 in historical keys
    decrypted = SharedBroker::Cipher.decrypt(encrypted, @registry, topic: "some.topic")
    assert_equal "old data", decrypted[:sensitive]
  end

  def test_client_integration_with_custom_key_provider
    in_memory = SharedBroker::Adapters::InMemory.new
    client = SharedBroker::Client.new(adapter: in_memory, key_provider: @registry)

    # Publish on general topic (uses v2)
    client.publish("general.topic", { name: "Bob" })
    raw_general = in_memory.published_messages("general.topic").first
    assert_equal "v2", raw_general[:_key_id]

    # Subscribe and decrypt automatically
    received = []
    client.subscribe("finance.payout", "queue") do |msg|
      received << msg
    end

    # Publish on finance topic (uses finance)
    client.publish("finance.payout", { amount: 100 })
    raw_finance = in_memory.published_messages("finance.payout").first
    assert_equal "finance", raw_finance[:_key_id]

    assert_equal 100, received.first[:amount]
  end
end
