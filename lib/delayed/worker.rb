module Delayed
  class Worker
    SLEEP = 5

    cattr_accessor :logger
    self.logger = if defined?(Merb::Logger)
      Merb.logger
    elsif defined?(RAILS_DEFAULT_LOGGER)
      RAILS_DEFAULT_LOGGER
    end

    def initialize(options={})
      @quiet = options[:quiet]
      Delayed::Job.min_priority = options[:min_priority] if options.has_key?(:min_priority)
      Delayed::Job.max_priority = options[:max_priority] if options.has_key?(:max_priority)
    end

    def start
      say "*** Starting job worker #{Delayed::Job.worker_name}"
      Delayed::Worker.divert_logging

      trap('TERM') { say 'Exiting...'; $exit = true }
      trap('INT')  { say 'Exiting...'; $exit = true }

      loop do
        result = nil

        realtime = Benchmark.realtime do
          result = Delayed::Job.work_off
        end

        count = result.sum

        break if $exit

        if count.zero?
          time("sleeping", 0.1) do  
            sleep(SLEEP)
          end
        else
          say "#{count} jobs processed at %.4f j/s, %d failed ..." % [count / realtime, result.last]
        end

        break if $exit
      end

    ensure
      Delayed::Job.clear_locks!
    end

    def say(text)
      puts text unless @quiet
      logger.info text if logger
    end

    # Make the workers spit out to log/dj_production.log
    def self.divert_logging
      const_set("RAILS_DEFAULT_LOGGER", ActiveSupport::BufferedLogger.new(File.join(Rails.root, 'log', "dj_#{Rails.env}.log")))
      RAILS_DEFAULT_LOGGER.level = ActiveSupport::BufferedLogger.const_get(Rails.configuration.log_level.to_s.upcase)
      RAILS_DEFAULT_LOGGER.auto_flushing = false if Rails.env.production?
      ActiveRecord::Base.logger = RAILS_DEFAULT_LOGGER
      ActiveRecord::Base.clear_active_connections!
    end
    
  end
end
