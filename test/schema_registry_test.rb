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
end
