# frozen_string_literal: true

module SharedBroker
  class MiddlewarePipeline
    def initialize(middlewares)
      @middlewares = Array(middlewares)
    end

    def execute(topic, message, metadata = {}, &block)
      run_middleware(0, topic, message, metadata, &block)
    end

    private

    def run_middleware(index, topic, message, metadata, &block)
      if index >= @middlewares.size
        yield
      else
        @middlewares[index].call(topic, message, metadata) do
          run_middleware(index + 1, topic, message, metadata, &block)
        end
      end
    end
  end
end
