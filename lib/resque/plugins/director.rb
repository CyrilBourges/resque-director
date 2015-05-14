
require 'resque-remora'
require 'resque/plugins/director/config'
require 'resque/plugins/director/scaler'

module Resque
  module Plugins
    module Director
      include Resque::Plugins::Remora
      include Resque::Plugins::Director::Scaler

      def attach_remora
        {:created_at => Time.now.utc.to_i}
      end

      def process_remora(queue, job)
        timestamp = job['created_at']
        start_time = timestamp.nil? ? Time.now.utc : Time.at(timestamp.to_i).utc
        after_pop_direct_workers(start_time)
      end

      def direct(options={})
        @config = Config.new(options)
      end

      def after_enqueue_scale_workers(*args)
        set_queue
        return if @config.no_enqueue_scale
        self.scale_within_requirements
      end

      def before_perform_direct_workers(*args)
        set_queue
        self.scale_within_requirements if @config.no_enqueue_scale
      end

      def after_pop_direct_workers(start_time=Time.now.utc)
        return unless scaling_config_set?
        set_queue

        time_through_queue = Time.now.utc - start_time
        jobs_in_queue = Resque.size(@queue.to_s)

        if scale_up?(time_through_queue, jobs_in_queue)
          self.scale_up
        elsif scale_down?(time_through_queue, jobs_in_queue)
          self.scale_down
        end
      end

      def after_perform_direct_workers(*args)
        set_queue
        jobs_in_queue = Resque.size(@queue.to_s)
        self.scale_down_to_minimum if jobs_in_queue == 0
      end

      def on_failure_direct_workers(*args)
        set_queue
        jobs_in_queue = Resque.size(@queue.to_s)
        self.scale_down_to_minimum if jobs_in_queue == 0
      end

      private

      def set_queue
        @config.queue ||= @queue.to_s
      end

      def scaling_config_set?
        @config.max_time > 0 || @config.max_queue > 0
      end

      def scale_up?(time_through_queue, jobs_in_queue)
        time_limits =  @config.max_time > 0 && time_through_queue > @config.max_time
        queue_limits = @config.max_queue > 0 && jobs_in_queue > @config.max_queue
        time_limits || queue_limits
      end

      def scale_down?(time_through_queue, jobs_in_queue)
        time_limits =  @config.max_time > 0 && time_through_queue < (@config.max_time/2)
        queue_limits = @config.max_queue > 0 && jobs_in_queue < (@config.max_queue/2)
        (@config.max_time <= 0 || time_limits) && (@config.max_queue <= 0 || queue_limits)
      end
    end
  end
end
