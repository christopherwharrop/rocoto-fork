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
  # A handle belongs to one thread. Nothing here is synchronized, and two
  # threads sharing a handle would interleave their requests on it.
  #
  ##########################################
  class Actor
    require 'json'
    require 'rbconfig'
    require 'socket'
    require 'timeout'
    require 'tmpdir'

    # Reserved, protocol-level message that stops the actor process itself
    # rather than being forwarded to the real served object.
    STOP_MESSAGE = "__actor_stop__".freeze

    # Bounds a single request read on the server side, so one hung/malformed
    # client can never wedge the actor's ability to serve everyone else.
    READ_TIMEOUT = 30

    # Bounds how long the actor will spend handing back one reply. A reply
    # bigger than the socket buffer can only be written as fast as the
    # caller reads it, and a caller that timed out is not reading until it
    # decides to wait again -- so this has to be generous enough to survive
    # that pause, while still guaranteeing the actor can never be wedged by
    # it forever. A caller whose process has died isn't a factor: writing to
    # it fails immediately rather than blocking.
    REPLY_TIMEOUT = 300

    # Bounds the polite "please exit" request. A healthy actor answers it in
    # milliseconds; anything slower is either busy with someone else's work
    # or wedged, and gets killed instead. Shutdown shouldn't wait on either.
    STOP_TIMEOUT = 5

    # Sockets live in node-local /tmp rather than under $HOME: home
    # directories on HPC systems are usually on shared filesystems (a hang
    # risk for the calling process), and are often long enough to push the
    # socket path past the kernel's ~104-108 byte limit.
    SOCKET_ROOT = "/tmp".freeze
    SOCKET_DIR_PREFIX = "rocoto-actor-".freeze

    RUNNER = File.expand_path("../../sbin/rocotoactor", __dir__)

    class ActorError < StandardError; end
    class ActorTimeout < ActorError; end
    class ActorUnavailable < ActorError; end
    class ActorBusy < ActorError; end

    # Raised when a request never arrived in full. It has its own class
    # because rocoto rescues Timeout::Error in many places to mean "the
    # scheduler was slow", and a transport problem must not be mistaken for
    # one of those.
    class RequestIncomplete < ActorError; end

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

      def monotonic
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end

      private

      def dispatch_one(conn, real_object, allowed)
        request = Timeout.timeout(READ_TIMEOUT, RequestIncomplete) { JSON.parse(conn.gets.to_s) }
        name = request["method"]

        if name == STOP_MESSAGE
          reply(conn, { "result" => true })
          return false
        end

        reply(conn, build_response(real_object, allowed, name, request["args"] || []))
        true
      rescue StandardError => e
        reply(conn, error_response(e))
        true
      # Anything that isn't a StandardError -- a failed require, runaway
      # recursion, the served code calling exit -- means this process can't
      # be trusted to keep serving. The caller still gets told what happened
      # before the exception is allowed to end us.
      rescue Exception => e # rubocop:disable Lint/RescueException
        reply(conn, fatal_response(e))
        raise
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

      # An anonymous exception class has no name, so send something the
      # caller can still make sense of rather than a null it would choke on.
      def error_response(error, prefix = "")
        { "error" => { "class" => error.class.name.to_s, "message" => "#{prefix}#{error.message}" } }
      end

      # Marks a reply as the last thing this actor will say. SystemExit's own
      # message is just "exit", which tells a caller nothing about what
      # happened, so describe it instead.
      def fatal_response(error)
        message = if error.is_a?(SystemExit)
                    "the served code called exit with status #{error.status}"
                  else
                    error.message
                  end
        { "error" => { "class" => error.class.name.to_s, "message" => message }, "fatal" => true }
      end

      ##########################################
      #
      # reply
      #
      # A result that can't be encoded (a string holding bytes that aren't
      # valid UTF-8, say) must still produce an answer. Staying silent would
      # look exactly like the actor dying, and would get a healthy actor
      # killed for what is really just an encoding problem.
      #
      ##########################################
      def reply(conn, response)
        payload = begin
          "#{JSON.generate(response)}\n"
        rescue StandardError => e
          "#{JSON.generate(error_response(e, 'reply could not be encoded: '))}\n"
        end
        write_reply(conn, payload)
      rescue StandardError
        nil
      end

      def write_reply(conn, payload)
        out = payload.dup.force_encoding(Encoding::BINARY)
        deadline = monotonic + REPLY_TIMEOUT
        until out.empty?
          remaining = deadline - monotonic
          break if remaining <= 0
          break unless IO.select(nil, [conn], nil, remaining)

          written = conn.write_nonblock(out, exception: false)
          out.slice!(0, written) unless written == :wait_writable
        end
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
    # Private: an Actor is only ever created by spawn, which is what gives
    # it a process to talk to.
    #
    ##########################################
    def initialize(klass, args, timeout: 150)
      reject_shadowed_methods!(klass)

      @klass = klass
      @args = args
      @timeout = timeout
      @allowed = klass.instance_methods - Object.instance_methods
      @abandoned = nil
      @pending = nil
      @reaped = false
    end
    private_class_method :new

    ##########################################
    #
    # method_missing
    #
    ##########################################
    def method_missing(name, *args, **kwargs, &block)
      return super unless @allowed.include?(name)

      # Both would be silently dropped on the way across, so say so instead.
      raise ActorError, "#{name} was given a block, which cannot be sent to an actor" if block
      raise ActorError, "#{name} was given keyword arguments, which an actor cannot carry yet" unless kwargs.empty?

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
      raise ActorError, "Actor #{@klass} (pid #{@pid}) has no call waiting for a reply" if @pending.nil?

      await(seconds)
    end

    ##########################################
    #
    # stop!
    #
    # Asks the actor to exit, and kills it if it doesn't answer promptly, or
    # if it is busy with a reply we already gave up on.
    #
    ##########################################
    def stop!
      return if @abandoned == :stopped

      exited = @abandoned.nil? && @pending.nil? && request_stop && reap(patient: true)
      @abandoned = :stopped
      terminate!(patient: true) unless exited
      self.class.remove_socket(@socket_path)
    end

    private

    ##########################################
    #
    # reject_shadowed_methods!
    #
    # A handle answers some calls itself -- its own methods, anything
    # inherited from Object, and the protocol's stop message. If the served
    # class defines any of those, calls to them would never reach the actor
    # and would quietly return the wrong thing, so refuse to spawn at all.
    #
    ##########################################
    def reject_shadowed_methods!(klass)
      defined_here = (klass.ancestors - Object.ancestors).flat_map { |mod| mod.instance_methods(false) }.uniq
      shadowed = defined_here & (Object.instance_methods + self.class.public_instance_methods(false) +
                                 [STOP_MESSAGE.to_sym])
      return if shadowed.empty?

      raise ArgumentError, "#{klass} defines #{shadowed.sort.join(', ')}, which an Actor handle answers itself; " \
                           "calls to those would never reach the actor"
    end

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
                           server.fileno.to_s, Process.pid.to_s, @socket_path, server => server)
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
    def call(name, args, timeout: @timeout)
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

      # Both buffers are measured in bytes, never characters: the socket
      # deals in bytes, and slicing a UTF-8 string by character count would
      # silently discard more of it than was actually sent.
      request = "#{JSON.generate({ 'method' => name, 'args' => args })}\n".force_encoding(Encoding::BINARY)
      @pending = { name: name, conn: conn, out: request,
                   buffer: +"".force_encoding(Encoding::BINARY), started_at: self.class.monotonic }
      await(timeout)
    end

    ##########################################
    #
    # await
    #
    # Sending and receiving are both bounded by the same deadline, since a
    # large request to an actor that has stopped reading could otherwise
    # block on the write instead of the read. The deadline is measured on
    # the monotonic clock, so an NTP step can't stretch or shorten it.
    #
    ##########################################
    def await(seconds)
      deadline = self.class.monotonic + seconds
      response = begin
        send_request(deadline)
        read_reply(deadline)
      rescue ReplyTimeout
        raise ActorTimeout, "Actor #{@klass} (pid #{@pid}) has not replied to #{@pending[:name]} after " \
                            "#{(self.class.monotonic - @pending[:started_at]).round} seconds; wait for it, " \
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
        return JSON.parse(line.force_encoding(Encoding::UTF_8)) if line

        raise ReplyTimeout unless IO.select([@pending[:conn]], nil, nil, time_left(deadline))

        # nil rather than an exception is how end-of-file arrives here: the
        # actor closed the connection, or died, before replying.
        chunk = @pending[:conn].read_nonblock(4096, exception: false)
        raise EOFError, "connection closed before a reply was received" if chunk.nil?

        @pending[:buffer] << chunk unless chunk == :wait_readable
      end
    end

    def time_left(deadline)
      remaining = deadline - self.class.monotonic
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
        Object.const_get(error["class"].to_s)
      rescue NameError, TypeError
        nil
      end
      error_class = RuntimeError unless reconstructable?(error_class)
      raise build_remote_error(error_class, error["message"])
    end

    def build_remote_error(error_class, message)
      exception = begin
        error_class.new(message)
      rescue StandardError
        # It can't be built from a message alone, so fall back to something
        # that can, keeping the real class name in the text.
        return RuntimeError.new("#{error_class}: #{message}")
      end

      # Errno classes prepend their own description to whatever message they
      # are handed, which would duplicate the text the actor already sent.
      # Keep the actor's wording exactly as it wrote it.
      if exception.message != message
        exception.define_singleton_method(:message) { message }
        exception.define_singleton_method(:to_s) { message }
      end
      exception
    end

    # Anything the actor can legitimately report, which is every error it
    # serializes, but never an exit or a signal: raising either of those in
    # the caller would end or interrupt it rather than tell it what happened.
    def reconstructable?(error_class)
      error_class.is_a?(Class) && error_class <= Exception &&
        !(error_class <= SystemExit) && !(error_class <= SignalException)
    end

    def request_stop
      call(STOP_MESSAGE, [], timeout: [@timeout, STOP_TIMEOUT].min)
      true
    rescue ActorError
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
    # before running any more of its own code.
    #
    # Nothing in here may raise: it runs while another exception is already
    # on its way to the caller, and would otherwise replace it.
    #
    ##########################################
    def terminate!(patient: false)
      begin
        @pending[:conn].close if @pending
      rescue SystemCallError, IOError
        nil
      end
      @pending = nil

      begin
        Process.kill("KILL", @pid) unless @reaped
      rescue SystemCallError
        nil
      end
      reap(patient: patient)
      self.class.remove_socket(@socket_path)
    end

    ##########################################
    #
    # reap
    #
    # Only waits around when shutting down. In the middle of a call there is
    # a deadline the caller chose, and collecting the corpse is never worth
    # overrunning it -- anything left unreaped is collected by init once
    # this process exits.
    #
    # Once reaped, @pid may be reused by an unrelated process, so this also
    # records that we must never signal it again.
    #
    ##########################################
    def reap(patient: false)
      return true if @reaped

      attempts = patient ? 10 : 1
      attempts.times do |attempt|
        unless Process.waitpid(@pid, Process::WNOHANG).nil?
          @reaped = true
          return true
        end
        sleep 0.1 unless attempt == attempts - 1
      end
      false
    rescue Errno::ECHILD
      @reaped = true
    end
  end
end
