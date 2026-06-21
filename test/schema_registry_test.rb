# frozen_string_literal: true

require "test_helper"

class SchemaRegistryTest < Minitest::Test
  def setup
    SharedBroker::SchemaRegistry.provider = nil
    SharedBroker::Validation.clear
  end

  def teardown
    SharedBroker::SchemaRegistry.provider = nil
    SharedBroker::Validation.clear
  end

  def test_local_provider_validation
    schema = Dry::Schema.Params do
      required(:id).filled(:integer)
    end
    
    SharedBroker::Validation.register("test.local", schema)
    SharedBroker::Validation.validate!("test.local", { id: 123 })

    assert_raises(SharedBroker::Validation::ValidationError) do
      SharedBroker::Validation.validate!("test.local", { id: "string" })
    end
  end

  def test_http_provider_fetches_and_validates
    http_provider = SharedBroker::SchemaRegistry::Providers::Http.new(
      url: "http://mock-registry.local",
      cache_ttl: 10
    )
    SharedBroker::SchemaRegistry.provider = http_provider

    json_schema = {
      "type" => "object",
      "required" => ["email"],
      "properties" => {
        "email" => { "type" => "string" }
      }
    }

    mock_response = Minitest::Mock.new
    mock_response.expect :is_a?, true, [Class]
    mock_response.expect :body, json_schema.to_json

    mock_http = Minitest::Mock.new
    mock_http.expect :request, mock_response, [Net::HTTP::Get]

    Net::HTTP.stub(:start, ->(_h, _p, _o = {}, &block) { block.call(mock_http) }) do
      SharedBroker::Validation.validate!("user.signup", { email: "user@example.com" })

      assert_raises(SharedBroker::Validation::ValidationError) do
        SharedBroker::Validation.validate!("user.signup", { email: 123 })
      end
    end

    mock_response.verify
    mock_http.verify
  end

  def test_http_provider_caching
    http_provider = SharedBroker::SchemaRegistry::Providers::Http.new(
      url: "http://mock-registry.local",
      cache_ttl: 60
    )

    json_schema = { "type" => "object" }

    mock_response = Minitest::Mock.new
    mock_response.expect :is_a?, true, [Class]
    mock_response.expect :body, json_schema.to_json

    mock_http = Minitest::Mock.new
    mock_http.expect :request, mock_response, [Net::HTTP::Get]

    Net::HTTP.stub(:start, ->(_h, _p, _o = {}, &block) { block.call(mock_http) }) do
      http_provider.validate!("cache.test", {})
      http_provider.validate!("cache.test", {})
    end

    mock_response.verify
    mock_http.verify
  end

  class MockCacheStore
    attr_reader :reads, :writes
    def initialize
      @store = {}
      @reads = []
      @writes = []
    end

    def read(key)
      @reads << key
      @store[key]
    end

    def write(key, value, expires_in: nil)
      @writes << { key: key, value: value, expires_in: expires_in }
      @store[key] = value
    end

    def exist?(key)
      @store.key?(key)
    end
  end

  def test_http_provider_with_shared_cache_store_hit
    store = MockCacheStore.new
    SharedBroker.cache_store = store
    
    # Pre-populate cache
    json_schema = { "type" => "object" }
    store.write("shared_broker:schema:cache.hit", json_schema)

    http_provider = SharedBroker::SchemaRegistry::Providers::Http.new(
      url: "http://mock-registry.local",
      cache_ttl: 60
    )

    # Validate should read from store and not trigger any Net::HTTP calls
    http_provider.validate!("cache.hit", {})

    assert_includes store.reads, "shared_broker:schema:cache.hit"
  ensure
    SharedBroker.cache_store = nil
  end

  def test_http_provider_with_shared_cache_store_miss
    store = MockCacheStore.new
    SharedBroker.cache_store = store

    http_provider = SharedBroker::SchemaRegistry::Providers::Http.new(
      url: "http://mock-registry.local",
      cache_ttl: 60
    )

    json_schema = { "type" => "object" }
    mock_response = Minitest::Mock.new
    mock_response.expect :is_a?, true, [Class]
    mock_response.expect :body, json_schema.to_json

    mock_http = Minitest::Mock.new
    mock_http.expect :request, mock_response, [Net::HTTP::Get]

    Net::HTTP.stub(:start, ->(_h, _p, _o = {}, &block) { block.call(mock_http) }) do
      http_provider.validate!("cache.miss", {})
    end

    assert_includes store.reads, "shared_broker:schema:cache.miss"
    assert_equal 1, store.writes.size
    assert_equal "shared_broker:schema:cache.miss", store.writes.first[:key]
    assert_equal json_schema, store.writes.first[:value]
  ensure
    SharedBroker.cache_store = nil
  end
end
