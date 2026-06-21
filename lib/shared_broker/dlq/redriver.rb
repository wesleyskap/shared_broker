# frozen_string_literal: true

module SharedBroker
  module DLQ
    class Redriver
      def self.redrive(client, dlq_name, original_topic, limit: nil)
        adapter = client.send(:resolve_adapter, original_topic)
        adapter.redrive_dlq(dlq_name, original_topic, limit: limit)
      end
    end
  end
end
