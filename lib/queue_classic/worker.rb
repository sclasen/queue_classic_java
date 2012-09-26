module QC
  class Executor < java.util.concurrent.ThreadPoolExecutor
    def initialize(a,b,c,d,e, semaphore)
      super(a,b,c,d,e)
      @semaphore = semaphore
    end

    def beforeExecute(thread, runnable)
      super
    end

    def afterExecute(runnable, throwable)
      @semaphore.release
      super
    end
  end

  class Worker

    attr_reader :queue

    def initialize(*args)
      if args.length == 5
        q_name, top_bound, fork_worker, listening_worker, max_attempts = *args
      elsif args.length <= 1
        opts = args.first || {}
        q_name           = opts[:q_name]           || QC::QUEUE
        top_bound        = opts[:top_bound]        || QC::TOP_BOUND
        fork_worker      = opts[:fork_worker]      || QC::FORK_WORKER
        listening_worker = opts[:listening_worker] || QC::LISTENING_WORKER
        max_attempts     = opts[:max_attempts]     || QC::MAX_LOCK_ATTEMPTS
      else
        raise ArgumentError, 'wrong number of arguments (expected no args, an options hash, or 5 separate args)'
      end

      @running = true
      @queue = Queue.new(q_name, listening_worker)
      @top_bound = top_bound
      @fork_worker = fork_worker
      @listening_worker = listening_worker
      @max_attempts = max_attempts

      @semaphore = java.util.concurrent.Semaphore.new(thread_pool_size)
      @pool = Executor.new(thread_pool_size, thread_pool_size, 0, java.util.concurrent.TimeUnit::MILLISECONDS, java.util.concurrent.LinkedBlockingQueue.new, @semaphore)

      handle_signals

      log(
        :level => :debug,
        :action => "worker_initialized",
        :queue => q_name,
        :top_bound => top_bound,
        :fork_worker => fork_worker,
        :listening_worker => listening_worker,
        :max_attempts => max_attempts
      )
    end

    def thread_pool_size
      10
    end

    def shutdown
      @pool.shutdown
    end

    def running?
      @running
    end

    def fork_worker?
      @fork_worker
    end

    def can_listen?
      @listening_worker
    end

    def handle_signals
      %W(INT TERM).each do |sig|
        trap(sig) do
          @pool.shutdown
          if running?
            @running = false
            log(:level => :debug, :action => "handle_signal", :running => @running)
          else
            raise Interrupt
          end
        end
      end
    end

    # This method should be overriden if
    # your worker is forking and you need to
    # re-establish database connectoins
    def setup_child
    end

    def start
      while running?
        if fork_worker?
          fork_and_work
        else
          work
        end
      end
    end

    def fork_and_work
      @cpid = fork { setup_child; work }
      log(:level => :debug, :action => :fork, :pid => @cpid)
      Process.wait(@cpid)
    end

    def work
      @semaphore.acquire # reserve a thread
      if job = lock_job
        @pool.submit do
          QC.log_yield(:level => :info, :action => "work_job", :job => job[:id]) do
            begin
              retval = call(job)
            rescue Object => e
              log(:level => :debug, :action => "failed_work", :job => job[:id], :error => e.inspect)
              handle_failure(job, e)
            ensure
              # Threads in the pool use the same connection as the 'listener' thread for deleting the job,
              # but this is OK since jdbc-postgres is threadsafe
              @queue.delete(job[:id])
              log(:level => :debug, :action => "delete_job", :job => job[:id])
            end
          end
        end
      else
        @semaphore.release
      end
    end

    def lock_job
      log(:level => :debug, :action => "lock_job")
      attempts = 0
      job = nil
      until job
        job = @queue.lock(@top_bound)
        if job.nil?
          log(:level => :debug, :action => "failed_lock", :attempts => attempts)
          if attempts < @max_attempts
            seconds = 2**attempts
            wait(seconds)
            attempts += 1
            next
          else
            break
          end
        else
          log(:level => :debug, :action => "finished_lock", :job => job[:id])
        end
      end
      job
    end

    def call(job)
      args = job[:args]
      klass = eval(job[:method].split(".").first)
      message = job[:method].split(".").last
      klass.send(message, *args)
    end

    def wait(t)
      if can_listen?
        log(:level => :debug, :action => "listen_wait", :wait => t)
        Conn.listen(@queue.chan)
        Conn.wait_for_notify(t)
        Conn.unlisten(@queue.chan)
        Conn.drain_notify
        log(:level => :debug, :action => "finished_listening")
      else
        log(:level => :debug, :action => "sleep_wait", :wait => t)
        Kernel.sleep(t)
      end
    end

    #override this method to do whatever you want
    def handle_failure(job,e)
      puts "!"
      puts "! \t FAIL"
      puts "! \t \t #{job.inspect}"
      puts "! \t \t #{e.inspect}"
      puts "!"
    end

    def log(data)
      QC.log(data)
    end

  end
end
