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
        end

        private

        def fetch_schema(topic)
          cached = @cache[topic.to_s]
          return cached[:schema] if cached && cached[:expires_at] > Time.now

          schema = download_schema(topic)
          @cache[topic.to_s] = { schema: schema, expires_at: Time.now + @cache_ttl } if schema
          schema
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
