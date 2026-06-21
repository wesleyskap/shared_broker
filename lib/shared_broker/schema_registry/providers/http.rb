# frozen_string_literal: true

require "net/http"
require "json"
require "json-schema"

module SharedBroker
  module SchemaRegistry
    module Providers
      class Http
        def initialize(url:, headers: {}, cache_ttl: 300)
          @base_url = url.chomp("/")
          @headers = headers
          @cache_ttl = cache_ttl
          @cache = {}
        end

        def validate!(topic, payload)
          schema = fetch_schema(topic)
          return unless schema

          begin
            JSON::Validator.validate!(schema, payload)
          rescue JSON::Schema::ValidationError => e
            raise SharedBroker::Validation::ValidationError,
                  "Schema validation failed for topic #{topic.inspect} against schema #{schema.inspect}. Offending payload: #{payload.inspect}. Error: #{e.message}"
          end
        end

        def clear_cache
          @cache.clear
          if SharedBroker.cache_store.respond_to?(:clear)
            SharedBroker.cache_store.clear
          end
        end

        private

        def fetch_schema(topic)
          cache_key = "shared_broker:schema:#{topic}"
          cached_schema = read_cache(cache_key)
          return cached_schema if cached_schema

          schema = download_schema(topic)
          write_cache(cache_key, schema) if schema
          schema
        end

        def read_cache(key)
          if SharedBroker.cache_store
            return SharedBroker.cache_store.read(key)
          end

          cached = @cache[key]
          return cached[:schema] if cached && cached[:expires_at] > Time.now
          nil
        end

        def write_cache(key, value)
          if SharedBroker.cache_store
            SharedBroker.cache_store.write(key, value, expires_in: @cache_ttl)
            return
          end

          @cache[key] = { schema: value, expires_at: Time.now + @cache_ttl }
        end

        def download_schema(topic)
          uri = URI("#{@base_url}/schemas/#{topic}.json")
          request = Net::HTTP::Get.new(uri)
          @headers.each { |k, v| request[k.to_s] = v.to_s }

          response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == "https") do |http|
            http.request(request)
          end

          return JSON.parse(response.body) if response.is_a?(Net::HTTPSuccess)

          nil
        end
      end
    end
  end
end
