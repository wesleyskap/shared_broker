# frozen_string_literal: true

require_relative "lib/shared_broker/version"

Gem::Specification.new do |spec|
  spec.name = "shared_broker"
  spec.version = SharedBroker::VERSION
  spec.authors = ["Gemini Antigravity"]
  spec.email = ["antigravity@google.com"]

  spec.summary = "Pluggable message broker abstraction with RabbitMQ and OpenTelemetry support."
  spec.description = "Shared library for asynchronous messaging, distributed tracing, and customizable adapters."
  spec.homepage = "https://github.com/onkai/shared_broker"
  spec.required_ruby_version = ">= 3.0.0"
  spec.license = "MIT"

  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "https://github.com/onkai/shared_broker/blob/main/CHANGELOG.md"


  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git appveyor Gemfile])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "bunny", "~> 2.22"
  spec.add_dependency "opentelemetry-api", "~> 1.2"
  spec.add_dependency "opentelemetry-sdk", "~> 1.2"
  spec.add_dependency "opentelemetry-exporter-otlp", "~> 0.25"
  spec.add_dependency "opentelemetry-instrumentation-all", "~> 0.25"
  spec.add_development_dependency "minitest", "~> 5.20"
  spec.add_development_dependency "rake", "~> 13.0"
end
