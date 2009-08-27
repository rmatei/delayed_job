namespace :dj do
  namespace :status do

    desc "Shows the most common errors"
    task :errors => :environment do
      result = Delayed::Job.connection.execute("select t1.last_error, t1.total_count from (select last_error, count(*) as total_count from delayed_jobs group by last_error order by count(*) DESC) t1 where t1.last_error is not null and t1.total_count>1;")
      result.each{|x| puts x}
    end

    desc "Prints the number of jobs in the queue"
    task :queue => :environment do
      puts ""

      total_jobs = Delayed::Job.count
      ready_to_run = Delayed::Job.count(:conditions => ["run_at < ? or run_at is null", Time.current.utc])
      puts "#{ready_to_run}/#{total_jobs} jobs are ready to run"

      being_run = Delayed::Job.count(:conditions => ["locked_at > ?", 5.minutes.ago.utc])
      puts "#{being_run} are being run"
    
      failed_jobs = Delayed::Job.count(:conditions => ["failed_at is not null"])
      jobs_with_failed_attempts = Delayed::Job.count(:conditions => ["attempts > 0"])
      puts "#{failed_jobs} failed jobs; #{jobs_with_failed_attempts} with failed attempts"
      puts "WARNING! destroying failed jobs" if Delayed::Job.destroy_failed_jobs
      
      start_time = Time.current
      Delayed::Job.find_available
      time_to_pull_jobs = Time.current - start_time    
      puts "#{time_to_pull_jobs.round(1)} sec. to query available jobs"
      
      # Figure out how long it'll take to burn through queue
      if ready_to_run > 1000
        puts "\nCalculating throughput..."
        sample_time = 10
        time_start = Time.current
        ready_to_run_1 = Delayed::Job.count(:conditions => ["run_at < ? or run_at is null", Time.current.utc])
        seconds_for_query = Time.current - time_start
        sleep(sample_time - seconds_for_query)
        ready_to_run_2 = Delayed::Job.count(:conditions => ["run_at < ? or run_at is null", Time.current.utc])
        throughput_per_hour = (ready_to_run_1 - ready_to_run_2) * 3600 / sample_time
        puts "#{total_jobs/throughput_per_hour} hours remaining (#{throughput_per_hour.round} jobs/hour)"
      end
        
    end

  end
end