namespace :dj do
  task :status do
    dj.processes
    dj.queue
  end

  desc "Show throughput"
  task :throughput, :roles => :primary_app do
    rake "dj:status:throughput"
  end
  
  desc "Show queue size"
  task :queue, :roles => :primary_app do
    rake "dj:status:queue"
  end
  
  desc "Calculate throughput 5 times"
  task :multi_throughput, :roles => :primary_app do
    8.times do
      run "cd #{current_path}; rake dj:status:throughput RAILS_ENV=production | grep Burning"
      sleep 15
    end
  end
  
  desc "Number of enqueued jobs"
  task :queue, :roles => :primary_app do
    rake "dj:status:queue"
  end
  
  desc "Show worker processes"
  task :processes, :roles => :app do
    run "ps aux | grep -v grep | grep -c 'rake jobs:work' || set $?=0"
  end
  
  desc "Show most common errors"
  task :errors, :roles => :primary_app do
    rake "dj:status:errors"
  end
  
  desc "Tail worker log file" 
  task :log, :roles => :app do
    run "tail -f #{shared_path}/log/dj_production.log" do |channel, stream, data|
      puts "#{data}" 
      break if stream == :err    
    end
  end
  
end