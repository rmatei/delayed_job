# Re-definitions are appended to existing tasks
task :environment
task :merb_env

namespace :jobs do
  desc "Clear the delayed_job queue."
  task :clear => [:merb_env, :environment] do
    Delayed::Job.delete_all
  end

  desc "Start a delayed_job worker."
  task :work => [:merb_env, :environment] do
    RAILS_DEFAULT_LOGGER = ActiveSupport::BufferedLogger.new(File.join(Rails.root, 'log', "dj_#{Rails.env}.log"))
    RAILS_DEFAULT_LOGGER.level = ActiveSupport::BufferedLogger.const_get(Rails.configuration.log_level.to_s.upcase)
    RAILS_DEFAULT_LOGGER.auto_flushing = false if Rails.env.production?
    ActiveRecord::Base.logger = RAILS_DEFAULT_LOGGER
    ActiveRecord::Base.clear_active_connections!
    Delayed::Worker.new(:min_priority => ENV['MIN_PRIORITY'], :max_priority => ENV['MAX_PRIORITY']).start
  end
end
