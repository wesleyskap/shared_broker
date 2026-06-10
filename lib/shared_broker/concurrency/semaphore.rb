# frozen_string_literal: true

require "thread"

module SharedBroker
  module Concurrency
    class Semaphore
      def initialize(limit)
        unless limit.is_a?(Integer) && limit > 0
          raise ArgumentError, "Expected limit to be a positive Integer, got: #{limit.inspect} (class: #{limit.class})"
        end

        @limit = limit
        @count = 0
        @mutex = Mutex.new
        @cond = ConditionVariable.new
      end

      def acquire
        @mutex.synchronize do
          while @count >= @limit
            @cond.wait(@mutex)
          end
          @count += 1
        end
      end

      def release
        @mutex.synchronize do
          @count -= 1
          @cond.signal
        end
      end

      def synchronize
        acquire
        begin
          yield
        ensure
          release
        end
      end
    end
  end
end
