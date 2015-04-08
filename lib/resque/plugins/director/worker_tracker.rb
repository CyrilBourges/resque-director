module Resque
  module Plugins
    module Director
      class WorkerTracker
        class << self

          def total_for_requirements
            start_number = workers_to_start
            return start_number if start_number > 0
            stop_number = workers_to_stop
            return stop_number if stop_number < 0
            0
          end

          def total_to_go_to_minimum
            to_minimum =  current_workers.size - Config.min_workers
            to_minimum > 0 ? to_minimum : 0
          end

          def total_to_add(number_to_start)
            return number_to_start if Config.max_workers <= 0
            scale_limit = Config.max_workers - current_workers.size
            Config.log("WORKER MAX REACHED: wanted to start #{number_to_start} workers on queue:#{Config.queue}") if scale_limit <= 0
            number_to_start > scale_limit ? scale_limit : number_to_start
          end

          def total_to_remove(number_to_stop)
            min_workers = Config.min_workers <= 0 ? 1 : Config.min_workers
            scale_limit = current_workers.size - min_workers
            if scale_limit <= 0 && Config.min_workers > 0
              Config.log("WORKER MIN REACHED: wanted to stop #{number_to_stop} workers on queue:#{Config.queue}")
            end
            number_to_stop > scale_limit ? scale_limit : number_to_stop
          end

          def valid_worker_pids
            valid_workers = current_workers.select{|w| w.hostname == `hostname`.chomp}
            valid_workers.map{|worker| worker.to_s.split(":")[1].to_i }
          end

          private

          def workers_to_start
            min_workers = Config.min_workers <= 0 ? 1 : Config.min_workers
            min_workers - current_workers.size
          end

          def workers_to_stop
            return 0 if Config.max_workers <= 0
            Config.max_workers - current_workers.size
          end

          def current_workers
            Resque.workers.select do |w|
              w.queues.map(&:to_s) == [Config.queue].flatten.map(&:to_s) && !w.shutdown?
            end
          end
        end
      end
    end
  end
end
