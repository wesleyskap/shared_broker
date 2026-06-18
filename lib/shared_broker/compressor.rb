# frozen_string_literal: true

require "zlib"

module SharedBroker
  module Compressor
    class CompressionError < StandardError; end
    class DecompressionError < StandardError; end

    SUPPORTED_ALGORITHMS = %w[gzip deflate].freeze

    def self.compress(data, algorithm)
      validate_algorithm!(algorithm)
      
      case algorithm.to_s
      when "gzip"
        Zlib.gzip(data)
      when "deflate"
        Zlib::Deflate.deflate(data)
      end
    rescue => e
      raise CompressionError, "Failed to compress data using #{algorithm.inspect}. Error: #{e.message}"
    end

    def self.decompress(data, algorithm)
      validate_algorithm!(algorithm)

      case algorithm.to_s
      when "gzip"
        Zlib.gunzip(data)
      when "deflate"
        Zlib::Inflate.inflate(data)
      end
    rescue => e
      raise DecompressionError, "Failed to decompress data using #{algorithm.inspect}. Error: #{e.message}"
    end

    def self.validate_algorithm!(algorithm)
      return if SUPPORTED_ALGORITHMS.include?(algorithm.to_s)

      raise ArgumentError, "Unsupported compression algorithm: #{algorithm.inspect}. Expected one of: #{SUPPORTED_ALGORITHMS.inspect}"
    end
    private_class_method :validate_algorithm!
  end
end
