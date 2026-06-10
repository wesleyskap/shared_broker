# frozen_string_literal: true

require "test_helper"

class ConcurrencyTest < Minitest::Test
  def setup
    @in_memory_adapter = SharedBroker::Adapters::InMemory.new
    @client = SharedBroker::Client.new(adapter: @in_memory_adapter)
  end

  def test_semaphore_limit
    semaphore = SharedBroker::Concurrency::Semaphore.new(2)
    active = 0
    threads = []
    mutex = Mutex.new

    5.times do
      threads << Thread.new do
        semaphore.synchronize do
          mutex.synchronize { active += 1 }
          sleep 0.05
          assert active <= 2
          mutex.synchronize { active -= 1 }
        end
      end
    end

    threads.each(&:join)
  end

  def test_semaphore_invalid_limit
    error = assert_raises(ArgumentError) do
      SharedBroker::Concurrency::Semaphore.new(0)
    end
    assert_match(/Expected limit to be a positive Integer, got: 0/, error.message)
  end

  def test_backpressure_throttling
    overloaded = true
    throttled_calls = 0

    limiter = SharedBroker::Concurrency::Limiter.new(
      backpressure_check: -> { overloaded },
      backpressure_backoff: 0.02
    )

    thread = Thread.new do
      limiter.run do
        throttled_calls += 1
      end
    end

    sleep 0.05
    assert_equal 0, throttled_calls

    overloaded = false
    thread.join

    assert_equal 1, throttled_calls
  end

  def test_subscribe_concurrency_limiting
    received = []
    mutex = Mutex.new
    max_active = 0
    current_active = 0

    @client.subscribe("concurrent.topic", "test_queue", max_concurrency: 2) do |msg|
      mutex.synchronize do
        current_active += 1
        max_active = [max_active, current_active].max
      end

      sleep 0.05

      mutex.synchronize do
        received << msg
        current_active -= 1
      end
    end

    threads = []
    4.times do |i|
      threads << Thread.new do
        @client.publish("concurrent.topic", { index: i })
      end
    end

    threads.each(&:join)

    assert_equal 4, received.size
    assert_operator max_active, :<=, 2
  end
end
