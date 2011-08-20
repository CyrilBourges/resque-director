module Resque
  module Plugins
    module Director
      class WorkerTracker
        attr_reader :workers
        
        def initialize
          @workers = current_workers
          @number_working = @workers.size
        end
          
        def total_for_requirements
          start_number = workers_to_start
          stop_number = workers_to_stop
          return start_number if start_number > 0 
          return stop_number if stop_number < 0 
          0
        end
      
        def total_to_add(number_to_start)
          return number_to_start if Config.max_workers <= 0
          scale_limit = Config.max_workers - @number_working
          number_to_start > scale_limit ? scale_limit : number_to_start
        end
        
        def total_to_remove(number_to_stop)
          scale_limit = @number_working - Config.min_workers
          number_to_stop > scale_limit ? scale_limit : number_to_stop
        end
        
        private
        
        def workers_to_start
          min_workers = Config.min_workers <= 0 ? 1 : Config.min_workers
          workers_to_start = min_workers - @number_working
        end
        
        def workers_to_stop
          return 0 if Config.max_workers <= 0
          workers_to_stop = Config.max_workers - @number_working
        end
        
        def current_workers
          Resque.workers.select {|w| w.queues == [Config.queue] }
        end
      end
    end
  end
end