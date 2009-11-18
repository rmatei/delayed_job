require 'timeout'
require "#{RAILS_ROOT}/vendor/plugins/rpm/init.rb"

module Delayed

  class DeserializationError < StandardError
  end

  # A job object that is persisted to the database.
  # Contains the work object as a YAML field.
  class Job < ActiveRecord::Base
    MAX_ATTEMPTS = 25
    MAX_RUN_TIME = 15.minutes
    set_table_name :delayed_jobs

    # By default failed jobs are destroyed after too many attempts.
    # If you want to keep them around (perhaps to inspect the reason
    # for the failure), set this to false.
    cattr_accessor :destroy_failed_jobs
    self.destroy_failed_jobs = false

    # Every worker has a unique name which by default is the pid of the process.
    # There are some advantages to overriding this with something which survives worker retarts:
    # Workers can safely resume working on tasks which are locked by themselves. The worker will assume that it crashed before.
    cattr_accessor :worker_name
    self.worker_name = "host:#{Socket.gethostname} pid:#{Process.pid}" rescue "pid:#{Process.pid}"

    NextTaskSQL         = '(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR (locked_by = ?)) AND failed_at IS NULL'
    NextTaskOrder       = 'priority DESC, run_at ASC'

    ParseObjectFromYaml = /\!ruby\/\w+\:([^\s]+)/

    cattr_accessor :min_priority, :max_priority
    self.min_priority = nil
    self.max_priority = nil

    # When a worker is exiting, make sure we don't have any locked jobs.
    def self.clear_locks!
      update_all("locked_by = null, locked_at = null", ["locked_by = ?", worker_name])
    end
    self.metaclass.add_method_tracer :clear_locks!

    def failed?
      failed_at
    end
    alias_method :failed, :failed?

    def payload_object
      @payload_object ||= deserialize(self['handler'])
    end

    def name
      @name ||= begin
        payload = payload_object
        if payload.respond_to?(:display_name)
          payload.display_name
        else
          payload.class.name
        end
      end
    end

    def payload_object=(object)
      self['handler'] = object.to_yaml
    end

    # Reschedule the job in the future (when a job fails).
    # Uses an exponential scale depending on the number of failed attempts.
    def reschedule(message, backtrace = [], time = nil)
      if self.attempts < MAX_ATTEMPTS
        time ||= Job.db_time_now + (attempts ** 4) + 5

        self.attempts    += 1
        self.run_at       = time
        self.last_error   = message + "\n" + backtrace.join("\n")
        self.unlock
        save!
      else
        logger.info "* [JOB] PERMANENTLY removing #{self.name} because of #{attempts} consequetive failures."
        destroy_failed_jobs ? destroy : update_attribute(:failed_at, Time.now)
      end
    end
    add_method_tracer :reschedule


    # Try to run one job. Returns true/false (work done/work failed) or nil if job can't be locked.
    def run_without_lock(worker_name)
      runtime = Benchmark.realtime do
        time("runnning job", 0.1) do  
          invoke_job # TODO: raise error if takes longer than max_run_time
        end
        time("destroying job", 0.1) do  
          destroy
        end
      end
      logger.info "* [JOB] #{name} completed after %.4f" % runtime
      return true  # did work
    rescue Exception => e
      time("rescheduling job", 0.1) do  
        reschedule e.message, e.backtrace
      end
      log_exception(e)
      return false  # work failed
    end
    add_method_tracer :run_without_lock

    # Add a job to the queue
    def self.enqueue(*args, &block)
      object = block_given? ? EvaledJob.new(&block) : args.shift

      unless object.respond_to?(:perform) || block_given?
        raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
      end
    
      priority = args.first || 0
      run_at   = args[1]

      Job.create(:payload_object => object, :priority => priority.to_i, :run_at => run_at)
    end
    self.metaclass.add_method_tracer :enqueue

    # Find a few candidate jobs to run (in case some immediately get locked by others).
    def self.find_available(limit = 5, max_run_time = MAX_RUN_TIME)

      time_now = db_time_now

      sql = NextTaskSQL.dup

      conditions = [time_now, time_now - max_run_time, worker_name]

      if self.min_priority
        sql << ' AND (priority >= ?)'
        conditions << min_priority
      end

      if self.max_priority
        sql << ' AND (priority <= ?)'
        conditions << max_priority
      end

      conditions.unshift(sql)

      ActiveRecord::Base.silence do
        # find(:all, :conditions => conditions, :order => NextTaskOrder, :limit => limit)
        find(:all, :conditions => conditions, :limit => limit)
      end
    end
    self.metaclass.add_method_tracer :find_available

    # Claim a few candidate jobs to run.
    def self.claim(limit = 5, max_run_time = MAX_RUN_TIME)
      time_now = db_time_now

      conditions = [NextTaskSQL.dup, time_now, time_now - max_run_time, worker_name]

      if min_priority
        conditions[0] << ' AND (priority >= ?)'
        conditions << min_priority
      end

      if max_priority
        conditions[0] << ' AND (priority <= ?)'
        conditions << max_priority
      end
      
      affected = time("locking #{limit} jobs", 0.1) do  
        begin
          offset = rand(200) * limit
          update_all(["locked_at = ?, locked_by = ?", time_now, worker_name], conditions, :limit => limit, :offset => offset)
        rescue Exception => e
          logger.error e.message
          @tries = @tries + 1 rescue 1
          sleep 1 
          if @tries <= 5
            puts "Retrying locking of jobs (#{e.message})..."
            retry
          end
          0
        end
      end
      if affected > 0
        time("finding locked jobs", 0.1) do  
          find(:all, :conditions => { :locked_at => time_now, :locked_by => worker_name })
        end
      else
        []
      end
    end
    self.metaclass.add_method_tracer :claim

    # Run the next job we can get an exclusive lock on.
    # If no jobs are left we return nil
    def self.claim_and_run(limit = 5, max_run_time = MAX_RUN_TIME)
      claim(limit, max_run_time).map do |job|
        job.run_without_lock(worker_name)
      end
    end
    self.metaclass.add_method_tracer :claim_and_run

    # Run the next job we can get an exclusive lock on.
    # If no jobs are left we return nil
    def self.reserve_and_run_one_job(max_run_time = MAX_RUN_TIME)

      # We get up to 5 jobs from the db. In case we cannot get exclusive access to a job we try the next.
      # this leads to a more even distribution of jobs across the worker processes
      find_available(5, max_run_time).each do |job|
        t = job.run_with_lock(max_run_time, worker_name)
        return t unless t == nil  # return if we did work (good or bad)
      end

      nil # we didn't do any work, all 5 were not lockable
    end
    self.metaclass.add_method_tracer :reserve_and_run_one_job

    # Lock this job for this worker.
    # Returns true if we have the lock, false otherwise.
    def lock_exclusively!(max_run_time, worker = worker_name)
      now = self.class.db_time_now
      affected_rows = if locked_by != worker
        # We don't own this job so we will update the locked_by name and the locked_at
        self.class.update_all(["locked_at = ?, locked_by = ?", now, worker], ["id = ? and (locked_at is null or locked_at < ?) and (run_at <= ?)", id, (now - max_run_time.to_i), now])
      else
        # We already own this job, this may happen if the job queue crashes.
        # Simply resume and update the locked_at
        self.class.update_all(["locked_at = ?", now], ["id = ? and locked_by = ?", id, worker])
      end
      if affected_rows == 1
        self.locked_at    = now
        self.locked_by    = worker
        return true
      else
        return false
      end
    end
    add_method_tracer :lock_exclusively!

    # Unlock this job (note: not saved to DB)
    def unlock
      self.locked_at    = nil
      self.locked_by    = nil
    end

    # This is a good hook if you need to report job processing errors in additional or different ways
    def log_exception(error)
      logger.error "* [JOB] #{name} failed with #{error.class.name}: #{error.message} - #{attempts} failed attempts"
      logger.error(error)
    end

    # Do num jobs in batches and return stats on success/failure.
    # Exit early if interrupted.
    def self.work_off(num = 100, batch_size = 25)
      success, failure = 0, 0

      batch_size = [num, batch_size].min
      (num / batch_size).times do
        results = claim_and_run(batch_size)
        break if $exit || results.empty?
        successes = results.select { |r| r }.size
        success += successes
        failure += results.size - successes
      end

      return [success, failure]
    end

    # Moved into its own method so that new_relic can trace it.
    def invoke_job
      payload_object.perform
    end

  private

    def deserialize(source)
      handler = YAML.load(source) rescue nil

      unless handler.respond_to?(:perform)
        if handler.nil? && source =~ ParseObjectFromYaml
          handler_class = $1
        end
        attempt_to_load(handler_class || handler.class)
        handler = YAML.load(source)
      end

      return handler if handler.respond_to?(:perform)

      raise DeserializationError,
        'Job failed to load: Unknown handler. Try to manually require the appropriate file.'
    rescue TypeError, LoadError, NameError => e
      raise DeserializationError,
        "Job failed to load: #{e.message}. Try to manually require the required file."
    end

    # Constantize the object so that ActiveSupport can attempt
    # its auto loading magic. Will raise LoadError if not successful.
    def attempt_to_load(klass)
       klass.constantize
    end

    # Get the current time (GMT or local depending on DB)
    # Note: This does not ping the DB to get the time, so all your clients
    # must have syncronized clocks.
    def self.db_time_now
      (ActiveRecord::Base.default_timezone == :utc) ? Time.now.utc : Time.now
    end

  protected

    def before_save
      self.run_at ||= self.class.db_time_now
    end

  end

  class EvaledJob
    def initialize
      @job = yield
    end

    def perform
      eval(@job)
    end
  end
end
