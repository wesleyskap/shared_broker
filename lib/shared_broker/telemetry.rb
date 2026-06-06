# frozen_string_literal: true

require "opentelemetry/sdk"
require "opentelemetry/instrumentation/all"

module SharedBroker
  module Telemetry
    def self.configure(service_name:)
      unless service_name.is_a?(String) && !service_name.empty?
        raise ArgumentError, "service_name must be a non-empty String, got #{service_name.inspect}"
      end

      OpenTelemetry::SDK.configure do |config|
        config.service_name = service_name
        config.use_all
      end
    end
  end
end
