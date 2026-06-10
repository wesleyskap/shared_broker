# frozen_string_literal: true

require_relative "semaphore"

module SharedBroker
  module Concurrency
    class Limiter
      def initialize(max_concurrency: nil, backpressure_check: nil, backpressure_backoff: 1.0)
        @backpressure_check = backpressure_check
        @backpressure_backoff = backpressure_backoff
        @semaphore = max_concurrency ? Semaphore.new(max_concurrency) : nil
      end

      def run
        wait_while_overloaded
        if @semaphore
          @semaphore.synchronize { yield }
        else
          yield
        end
      end

      private

      def wait_while_overloaded
        return unless @backpressure_check

        while @backpressure_check.call
          sleep(@backpressure_backoff)
        end
      end
    end
  end
end
