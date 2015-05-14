
require 'resque/plugins/director/worker_tracker'

module Resque
  module Plugins
    module Director
      module Scaler
        include Resque::Plugins::Director::WorkerTracker

        def scale_up(number_of_workers=1)
          number_of_workers = self.total_to_add(number_of_workers)
          scaling(number_of_workers) do
            start(number_of_workers)
          end
        end

        def scale_down(number_of_workers=1)
          number_of_workers = self.total_to_remove(number_of_workers)
          scaling(number_of_workers) do
            stop(number_of_workers)
          end
        end

        def scale_down_to_minimum
          number_of_workers = self.total_to_go_to_minimum
          stop(number_of_workers)
        end

        def scale_within_requirements
          number_of_workers = self.total_for_requirements
          if number_of_workers > 0
            set_last_scaled unless start(number_of_workers) == false
          elsif number_of_workers < 0
            set_last_scaled unless stop(number_of_workers * -1) == false
          end
        end

        def scaling(number_of_workers=1)
          return unless time_to_scale? && number_of_workers > 0
          set_last_scaled unless yield == false
        end

        private

        def set_last_scaled
          Resque.redis.set("last_scaled_#{[@config.queue].flatten.join('')}", Time.now.utc.to_i)
        end

        def time_to_scale?
          last_time = Resque.redis.get("last_scaled_#{[@config.queue].flatten.join('')}")
          return true if last_time.nil?
          time_passed = (Time.now.utc - Time.at(last_time.to_i).utc)
          time_passed >= @config.wait_time
        end

        def start(number_of_workers)
          @config.log("starting #{number_of_workers} workers on queue:#{@config.queue}") if number_of_workers > 0
          return override(number_of_workers, @config.start_override) if @config.start_override
          start_default(number_of_workers)
        end

        def stop(number_of_workers)
          @config.log("stopping #{number_of_workers} workers on queue:#{@config.queue}") if number_of_workers > 0
          return override(number_of_workers, @config.stop_override) if @config.stop_override
          stop_default(number_of_workers)
        end

        def override(number_of_workers, override_block)
          number_of_workers.times {override_block.call(@config.queue) }
        end

        def start_default(number_of_workers)
          number_of_workers.times { system("QUEUE=#{[@config.queue].flatten.join(",")} rake resque:work &") }
        end

        def stop_default(number_of_workers)
          worker_pids = self.valid_worker_pids[0...number_of_workers]
          worker_pids.each do |pid|
            Process.kill("QUIT", pid) rescue nil
          end
        end
      end
    end
  end
end
