##########################################
#
# Module WorkflowMgr
#
##########################################
module WorkflowMgr
  ##########################################
  #
  # Class Actor
  #
  # A handle to an object running in its own, isolated OS process (spawned
  # via fork+exec), reachable only from the local machine over a Unix domain
  # socket. Method calls are transparently forwarded to the real object and
  # its result (or exception) comes back as though the call had happened
  # in-process. This is what provides fault isolation: a hang in the actor
  # -- including an uninterruptible filesystem hang -- can never block
  # whatever holds this handle past its own timeout.
  #
  # A timeout means "no reply yet", not "dead". Deciding that a slow actor
  # is beyond hope is a judgement call that depends on what it was asked to
  # do -- a scheduler under heavy load can legitimately take minutes -- so
  # the actor is left running and the caller chooses: wait for the same
  # reply again, or stop! to give up and kill it. While a reply is
  # outstanding, calls to anything else fail immediately rather than queue,
  # so a slow actor still can't hold up the process that's talking to it.
  #
  ##########################################
  class Actor
    require 'json'
    require 'socket'
    require 'timeout'
    require 'tmpdir'

    # Reserved, protocol-level message that stops the actor process itself
    # rather than being forwarded to the real served object.
    STOP_MESSAGE = "__actor_stop__".freeze

    # Bounds a single request read on the server side, so one hung/malformed
    # client can never wedge the actor's ability to serve everyone else.
    READ_TIMEOUT = 30

    # Sockets live in node-local /tmp rather than under $HOME: home
    # directories on HPC systems are usually on shared filesystems (a hang
    # risk for the calling process), and are often long enough to push the
    # socket path past the kernel's ~104-108 byte limit.
    SOCKET_ROOT = "/tmp".freeze
    SOCKET_DIR_PREFIX = "rocoto-actor-".freeze

    RUNNER = File.expand_path("../../sbin/rocotoactor", __dir__)

    # Bugs in the served code, such as a failed require or runaway
    # recursion. Code that is broken must not keep running, but the caller
    # still needs the real error rather than a bare "the actor vanished",
    # so these are reported back first and the actor exits afterwards.
    FATAL_ERRORS = [ScriptError, SystemStackError].freeze

    class ActorTimeout < StandardError; end
    class ActorUnavailable < StandardError; end
    class ActorBusy < StandardError; end

    # Raised only by our own wait for a reply, so it can never be confused
    # with a Timeout::Error that the served object raised and sent back.
    class ReplyTimeout < StandardError; end
    private_constant :ReplyTimeout

    class << self
      ##########################################
      #
      # spawn
      #
      ##########################################
      def spawn(klass, *args, timeout: 150)
        actor = new(klass, args, timeout: timeout)
        actor.send(:launch!)
        actor
      end

      ##########################################
      #
      # serve
      #
      # Runs forever, dispatching requests from `server` to `real_object`.
      # Called by the generic runner script (sbin/rocotoactor) after it has
      # detached and constructed the real object.
      #
      # A request that arrives after our parent is gone was sent by a rocoto
      # process that no longer exists (e.g. queued while we were hung), and
      # acting on it could change state that a newer rocoto process now
      # owns. So instead of serving it, we stop.
      #
      ##########################################
      def serve(server, real_object, parent_pid: nil)
        allowed = real_object.class.instance_methods - Object.instance_methods
        loop do
          conn = server.accept
          unless parent_pid.nil? || Process.ppid == parent_pid
            conn.close
            break
          end
          break unless dispatch_one(conn, real_object, allowed)
        end
      ensure
        remove_socket(server.addr[1])
      end

      ##########################################
      #
      # serve_startup_failure
      #
      # Used when the served object could not be constructed at all. Waits
      # for the first caller and hands it the reason, so it sees the real
      # error instead of an unexplained connection failure, then returns so
      # the actor can exit. There is deliberately no time limit on that
      # wait: an actor is often spawned well before its first call, and the
      # parent watchdog already bounds how long we can linger.
      #
      ##########################################
      def serve_startup_failure(server, error)
        conn = server.accept
        conn.gets # the request itself is moot; nothing was ever constructed to serve it
        reply(conn, error_response(error, "could not be started: ").merge("fatal" => true))
        conn.close
      rescue StandardError
        nil
      ensure
        remove_socket(server.addr[1])
      end

      ##########################################
      #
      # watch_parent!
      #
      # Runs the given block (default: terminate this process immediately)
      # once parent_pid is no longer our parent. This is the backstop that
      # works even if the parent was killed with SIGKILL and never got a
      # chance to tell us to stop.
      #
      # Checking Process.ppid rather than whether parent_pid is alive matters:
      # we are reparented the instant our parent exits, even while it lingers
      # as an unreaped zombie, and a reused pid can't fool the check.
      #
      ##########################################
      def watch_parent!(parent_pid, poll_interval: 10, &on_parent_gone)
        on_parent_gone ||= -> { exit!(0) }
        Thread.new do
          sleep poll_interval while Process.ppid == parent_pid
          on_parent_gone.call
        end
      end

      ##########################################
      #
      # remove_socket
      #
      # Removes a socket file and the private directory created for it. Safe
      # to call more than once, and from either side of the socket.
      #
      ##########################################
      def remove_socket(path)
        return if path.nil? || path.empty?

        File.delete(path) if File.exist?(path)
        dir = File.dirname(path)
        Dir.rmdir(dir) if File.basename(dir).start_with?(SOCKET_DIR_PREFIX)
      rescue SystemCallError
        nil
      end

      private

      def dispatch_one(conn, real_object, allowed)
        request = Timeout.timeout(READ_TIMEOUT) { JSON.parse(conn.gets.to_s) }
        name = request["method"]

        if name == STOP_MESSAGE
          reply(conn, { "result" => true })
          return false
        end

        reply(conn, build_response(real_object, allowed, name, request["args"] || []))
        true
      rescue *FATAL_ERRORS => e
        # Report it, then let it end this process: the reply is flushed by
        # the ensure below before the exception unwinds any further.
        reply(conn, error_response(e).merge("fatal" => true))
        raise
      rescue StandardError => e
        reply(conn, error_response(e))
        true
      ensure
        conn.close
      end

      def build_response(real_object, allowed, name, args)
        unless allowed.include?(name.to_s.to_sym)
          return { "error" => { "class" => "NoMethodError", "message" => "#{name} is not permitted" } }
        end

        { "result" => real_object.public_send(name, *args) }
      rescue StandardError => e
        error_response(e)
      end

      def error_response(error, prefix = "")
        { "error" => { "class" => error.class.name, "message" => "#{prefix}#{error.message}" } }
      end

      def reply(conn, response)
        conn.puts(JSON.generate(response))
      rescue StandardError
        nil
      end

      # Dir.mktmpdir creates a brand new directory, mode 0700, with an
      # unguessable name, and fails rather than reuse an existing one. That
      # is what makes a shared, world-writable /tmp safe here: nobody else
      # can have pre-created, or later swap out, anything inside it.
      def new_socket_path(klass)
        prefix = "#{SOCKET_DIR_PREFIX}#{klass.name.split('::').last}-"
        File.join(Dir.mktmpdir(prefix, SOCKET_ROOT), "actor.sock")
      end
    end

    ##########################################
    #
    # initialize
    #
    ##########################################
    def initialize(klass, args, timeout: 150)
      @klass = klass
      @args = args
      @timeout = timeout
      @allowed = klass.instance_methods - Object.instance_methods
      @abandoned = nil
      @pending = nil
      @reaped = false
    end

    ##########################################
    #
    # method_missing
    #
    ##########################################
    def method_missing(name, *args)
      return super unless @allowed.include?(name)

      call(name, args)
    end

    def respond_to_missing?(name, include_private = false)
      @allowed.include?(name) || super
    end

    ##########################################
    #
    # wait
    #
    # Keeps waiting for a reply that an earlier call gave up on, for up to
    # another `seconds`. Returns that call's result, or raises ActorTimeout
    # again, leaving the reply outstanding so it can be waited on once more.
    #
    ##########################################
    def wait(seconds = @timeout)
      raise ActorUnavailable, "Actor #{@klass} (pid #{@pid}) #{abandoned_reason}" if @abandoned
      raise "Actor #{@klass} (pid #{@pid}) has no call waiting for a reply" if @pending.nil?

      await(seconds)
    end

    ##########################################
    #
    # stop!
    #
    # Asks the actor to exit, and kills it if it doesn't, or if it is busy
    # with a reply we already gave up on. Never waits longer than one
    # call's timeout.
    #
    ##########################################
    def stop!
      return if @abandoned == :stopped

      exited = @abandoned.nil? && @pending.nil? && request_stop && reap
      @abandoned = :stopped
      terminate! unless exited
      self.class.remove_socket(@socket_path)
    end

    private

    ##########################################
    #
    # launch!
    #
    ##########################################
    def launch!
      @socket_path = self.class.send(:new_socket_path, @klass)
      server = UNIXServer.new(@socket_path)
      File.chmod(0o600, @socket_path)

      # The runner starts as a fresh interpreter that has never loaded the
      # served class, so it needs to know which file defines it. Since the
      # caller already had to load @klass to reference it here, we can look
      # that up ourselves instead of asking the developer for it.
      source_file = Object.const_source_location(@klass.name)&.first

      # Process.spawn forks and execs without running any Ruby code in
      # between, which keeps this safe even when the caller has other
      # threads running (a plain fork could copy a mutex another thread
      # holds). The listening socket is the only descriptor passed on.
      @pid = Process.spawn(RbConfig.ruby, RUNNER, @klass.name, source_file.to_s, JSON.generate(@args),
                           server.fileno.to_s, Process.pid.to_s, server => server)
    rescue StandardError
      self.class.remove_socket(@socket_path)
      raise
    ensure
      server&.close
    end

    ##########################################
    #
    # call
    #
    ##########################################
    def call(name, args)
      raise ActorUnavailable, "Actor #{@klass} (pid #{@pid}) #{abandoned_reason}" if @abandoned

      if @pending
        raise ActorBusy, "Actor #{@klass} (pid #{@pid}) is still working on #{@pending[:name]}; " \
                         "wait for that reply, or stop! to give up on it"
      end

      begin
        conn = UNIXSocket.new(@socket_path)
      rescue SystemCallError, IOError => e
        abandon!("became unavailable: #{e.message}")
        raise ActorUnavailable, "Actor #{@klass} (pid #{@pid}) #{abandoned_reason}"
      end

      @pending = { name: name, conn: conn, out: "#{JSON.generate({ 'method' => name, 'args' => args })}\n",
                   buffer: +"", started_at: Time.now }
      await(@timeout)
    end

    ##########################################
    #
    # await
    #
    # Sending and receiving are both bounded by the same deadline, since a
    # large request to an actor that has stopped reading could otherwise
    # block on the write instead of the read.
    #
    ##########################################
    def await(seconds)
      deadline = Time.now + seconds
      response = begin
        send_request(deadline)
        read_reply(deadline)
      rescue ReplyTimeout
        raise ActorTimeout, "Actor #{@klass} (pid #{@pid}) has not replied to #{@pending[:name]} after " \
                            "#{(Time.now - @pending[:started_at]).round} seconds; wait for it, " \
                            "or stop! to give up on it"
      rescue SystemCallError, IOError, JSON::ParserError => e
        abandon!("became unavailable: #{e.message}")
        raise ActorUnavailable, "Actor #{@klass} (pid #{@pid}) #{abandoned_reason}"
      end

      finish_request
      handle_response(response)
    end

    def send_request(deadline)
      out = @pending[:out]
      until out.empty?
        raise ReplyTimeout unless IO.select(nil, [@pending[:conn]], nil, time_left(deadline))

        written = @pending[:conn].write_nonblock(out, exception: false)
        out.slice!(0, written) unless written == :wait_writable
      end
    end

    def read_reply(deadline)
      loop do
        line = @pending[:buffer].slice!(/\A[^\n]*\n/)
        return JSON.parse(line) if line

        raise ReplyTimeout unless IO.select([@pending[:conn]], nil, nil, time_left(deadline))

        # nil rather than an exception is how end-of-file arrives here: the
        # actor closed the connection, or died, before replying.
        chunk = @pending[:conn].read_nonblock(4096, exception: false)
        raise EOFError, "connection closed before a reply was received" if chunk.nil?

        @pending[:buffer] << chunk unless chunk == :wait_readable
      end
    end

    def time_left(deadline)
      remaining = deadline - Time.now
      raise ReplyTimeout if remaining <= 0

      remaining
    end

    def finish_request
      @pending[:conn].close
      @pending = nil
    end

    def handle_response(response)
      if response.key?("error")
        # The actor is on its way out, so there is nothing left to talk to.
        abandon!("exited after the served code raised #{response['error']['class']}") if response["fatal"]
        raise_remote_error(response["error"])
      end

      response["result"]
    end

    def raise_remote_error(error)
      error_class = begin
        Object.const_get(error["class"])
      rescue NameError
        nil
      end
      error_class = RuntimeError unless reconstructable?(error_class)
      exception = begin
        error_class.new(error["message"])
      rescue StandardError
        RuntimeError.new("#{error['class']}: #{error['message']}")
      end
      raise exception
    end

    # Anything the actor can legitimately report, which is every error it
    # serializes, but never an exit or a signal.
    def reconstructable?(error_class)
      error_class.is_a?(Class) && error_class <= Exception &&
        !(error_class <= SystemExit) && !(error_class <= SignalException)
    end

    def request_stop
      call(STOP_MESSAGE, [])
      true
    rescue ActorUnavailable, ActorTimeout
      false
    end

    def abandon!(reason)
      @abandoned = reason
      terminate!
    end

    def abandoned_reason
      @abandoned == :stopped ? "has been stopped" : @abandoned
    end

    ##########################################
    #
    # terminate!
    #
    # SIGKILL can't be caught or ignored. A process in uninterruptible sleep
    # won't act on it until its current syscall returns, but it then dies
    # before running any more of its own code. We wait at most a second to
    # reap it; one still stuck after that is left for init to reap.
    #
    ##########################################
    def terminate!
      begin
        @pending[:conn].close if @pending
      rescue IOError
        nil
      end
      @pending = nil

      begin
        Process.kill("KILL", @pid) unless @reaped
      rescue Errno::ESRCH
        nil
      end
      reap
      self.class.remove_socket(@socket_path)
    end

    ##########################################
    #
    # reap
    #
    # Once reaped, @pid may be reused by an unrelated process, so this also
    # records that we must never signal it again.
    #
    ##########################################
    def reap
      return true if @reaped

      10.times do
        unless Process.waitpid(@pid, Process::WNOHANG).nil?
          @reaped = true
          return true
        end
        sleep 0.1
      end
      false
    rescue Errno::ECHILD
      @reaped = true
    end
  end
end
