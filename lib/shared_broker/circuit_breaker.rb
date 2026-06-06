# frozen_string_literal: true

require "thread"

module SharedBroker
  class CircuitBreaker
    class OpenError < StandardError; end

    attr_reader :state, :failure_threshold, :recovery_timeout, :failure_count

    def initialize(failure_threshold: 5, recovery_timeout: 30)
      @failure_threshold = failure_threshold
      @recovery_timeout = recovery_timeout
      @state = :closed
      @failure_count = 0
      @last_failure_time = nil
      @mutex = Mutex.new
    end

    def run
      check_state!

      begin
        result = yield
        success!
        result
      rescue => e
        record_failure!
        raise e
      end
    end

    private

    def check_state!
      @mutex.synchronize do
        if @state == :open
          if Time.now.utc - @last_failure_time > @recovery_timeout
            @state = :half_open
          else
            raise OpenError, "Circuit is open. Refusing to execute command."
          end
        end
      end
    end

    def success!
      @mutex.synchronize do
        if @state == :half_open || @state == :closed
          @state = :closed
          @failure_count = 0
        end
      end
    end

    def record_failure!
      @mutex.synchronize do
        @failure_count += 1
        @last_failure_time = Time.now.utc
        if @failure_count >= @failure_threshold
          @state = :open
        end
      end
    end
  end
end
