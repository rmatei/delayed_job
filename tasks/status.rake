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
      start_time = Time.current
      Delayed::Job.find_available
      time_to_pull_jobs = Time.current - start_time    
      puts "#{time_to_pull_jobs.round(1)} sec. to query available jobs"

      total_jobs = Delayed::Job.count
      puts "#{total_jobs} jobs in queue"

      ready_to_run = Delayed::Job.count(:conditions => ["run_at < ? or run_at is null", Time.current.utc])
      puts "#{ready_to_run} are ready to run, #{total_jobs - ready_to_run} scheduled for later"

      being_run = Delayed::Job.count(:conditions => ["locked_at > ?", 5.minutes.ago.utc])
      puts "#{being_run} are being run"
    
      failed_jobs = Delayed::Job.count(:conditions => ["failed_at is not null"])
      jobs_with_failed_attempts = Delayed::Job.count(:conditions => ["attempts > 0"])
      puts "#{failed_jobs} failed jobs; #{jobs_with_failed_attempts} with failed attempts (destroying failed jobs: #{Delayed::Job.destroy_failed_jobs})"
    end

  end
end