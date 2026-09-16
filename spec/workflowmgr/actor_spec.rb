# frozen_string_literal: true

require 'spec_helper'
require 'json'
require 'socket'
require 'workflowmgr/actor'
require_relative 'support/echo_actor_test_double'
require_relative 'support/failing_actor_test_double'

RSpec.describe WorkflowMgr::Actor do
  def alive?(pid)
    Process.kill(0, pid)
    true
  rescue Errno::ESRCH
    false
  end

  def wait_until_dead(pid, within:)
    deadline = Time.now + within
    sleep 0.1 while alive?(pid) && Time.now < deadline
    !alive?(pid)
  end

  it 'forwards method calls to the real object running in its own process' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    expect(actor.greet('!')).to eq('hello world!')
  ensure
    actor&.stop!
  end

  it 're-raises the same exception class the served object raised' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    expect { actor.boom }.to raise_error(ArgumentError, 'kaboom')
  ensure
    actor&.stop!
  end

  it 'rejects methods not defined on the served class' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    expect { actor.not_a_real_method }.to raise_error(NoMethodError)
  ensure
    actor&.stop!
  end

  it 'passes along system errors the served object raises without treating the actor as failed' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    expect { actor.read_missing_file }.to raise_error(Errno::ENOENT)
    expect(actor.greet('!')).to eq('hello world!')
  ensure
    actor&.stop!
  end

  it 'leaves a slow actor running on timeout, so the caller can decide to keep waiting' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 1)
    pid = actor.instance_variable_get(:@pid)

    expect { actor.nap(3) }.to raise_error(WorkflowMgr::Actor::ActorTimeout)
    expect(alive?(pid)).to be true

    # The same reply, still outstanding rather than lost: waiting longer
    # turns an unknown outcome back into a known one.
    expect(actor.wait(10)).to eq('awake')
    expect(actor.greet('!')).to eq('hello world!')
  ensure
    actor&.stop!
  end

  it 'refuses other calls while a reply is outstanding, rather than queueing behind it' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 1)
    expect { actor.nap(3) }.to raise_error(WorkflowMgr::Actor::ActorTimeout)

    started_at = Time.now
    expect { actor.greet('!') }.to raise_error(WorkflowMgr::Actor::ActorBusy)
    expect(Time.now - started_at).to be < 0.5
  ensure
    actor&.stop!
  end

  it 'kills an actor whose caller gives up on it' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 1)
    pid = actor.instance_variable_get(:@pid)
    socket_dir = File.dirname(actor.instance_variable_get(:@socket_path))
    expect { actor.nap(30) }.to raise_error(WorkflowMgr::Actor::ActorTimeout)

    actor.stop!

    expect(alive?(pid)).to be false
    expect(File.exist?(socket_dir)).to be false
  end

  it 'tells the caller why it could not start, instead of just vanishing' do
    actor = described_class.spawn(FailingActorTestDouble, 'world', timeout: 5)
    pid = actor.instance_variable_get(:@pid)

    expect { actor.greet }.to raise_error(ArgumentError, /could not be started: cannot be constructed/)
    expect(wait_until_dead(pid, within: 3)).to be true
  ensure
    actor&.stop!
  end

  it 'reports a programming error to the caller and then exits, since its code cannot be trusted' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    pid = actor.instance_variable_get(:@pid)

    expect { actor.bad_require }.to raise_error(LoadError)
    expect(wait_until_dead(pid, within: 3)).to be true
    expect { actor.greet('!') }.to raise_error(WorkflowMgr::Actor::ActorUnavailable)
  ensure
    actor&.stop!
  end

  it 'does not relaunch an actor that died, since a fresh one would lack its state' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    Process.kill('KILL', actor.instance_variable_get(:@pid))
    expect { actor.greet('!') }.to raise_error(WorkflowMgr::Actor::ActorUnavailable)
  ensure
    actor&.stop!
  end

  it 'reports an actor that dies in the middle of a request as unavailable' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    expect { actor.die }.to raise_error(WorkflowMgr::Actor::ActorUnavailable)
  ensure
    actor&.stop!
  end

  it 'keeps its socket in a private directory under /tmp and removes both when stopped' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    path = actor.instance_variable_get(:@socket_path)
    dir = File.dirname(path)

    expect(dir).to start_with('/tmp/rocoto-actor-EchoActorTestDouble-')
    expect(File.stat(dir).uid).to eq(Process.uid)
    expect(File.stat(dir).mode & 0o777).to eq(0o700)
    expect(File.stat(path).mode & 0o777).to eq(0o600)

    actor.stop!
    expect(File.exist?(path)).to be false
    expect(File.exist?(dir)).to be false
  ensure
    actor&.stop!
  end

  it 'stays responsive even if the actor is completely frozen, simulating an unkillable hang' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 1)
    pid = actor.instance_variable_get(:@pid)
    socket_dir = File.dirname(actor.instance_variable_get(:@socket_path))

    # SIGSTOP can't be trapped, blocked, or ignored -- it freezes the process
    # at the kernel level. A real D-state hang can't be conjured on demand,
    # but this produces the exact property that matters here: the actor
    # cannot respond to anything, no matter what, until SIGCONT.
    Process.kill('STOP', pid)

    started_at = Time.now
    expect { actor.greet('!') }.to raise_error(WorkflowMgr::Actor::ActorTimeout)

    # Bounded by the actor's own timeout, never by however long the freeze
    # lasts -- this is what proves the main process can't be hung by it.
    expect(Time.now - started_at).to be < 3

    # And nothing about the frozen actor stops a separate, healthy actor from
    # working normally -- true isolation, not just "this one call gave up".
    other_actor = described_class.spawn(EchoActorTestDouble, 'someone else', timeout: 5)
    expect(other_actor.greet('!')).to eq('hello someone else!')

    # SIGKILL works even on a stopped process, so giving up gets rid of the
    # frozen actor and its socket, without ever having waited on it.
    actor.stop!
    expect(alive?(pid)).to be false
    expect(File.exist?(socket_dir)).to be false
  ensure
    other_actor&.stop!
    actor&.stop!
  end

  it 'exits its own process once stopped, without leaving a zombie behind' do
    actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
    pid = actor.instance_variable_get(:@pid)
    actor.stop!
    expect(alive?(pid)).to be false
  end

  it 'self-terminates if its parent disappears, even via SIGKILL with no chance to send stop!' do
    rd, wr = IO.pipe
    helper_pid = fork do
      rd.close
      helper_actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
      wr.puts(helper_actor.instance_variable_get(:@pid))
      wr.close
      sleep 30
    end
    wr.close
    actor_pid = rd.gets.to_i
    rd.close

    Process.kill('KILL', helper_pid)
    Process.wait(helper_pid)

    expect(wait_until_dead(actor_pid, within: 15)).to be true
  end

  it 'refuses requests sent after its parent died, instead of acting on them' do
    rd, wr = IO.pipe
    helper_pid = fork do
      rd.close
      helper_actor = described_class.spawn(EchoActorTestDouble, 'world', timeout: 5)
      helper_actor.greet # the actor is up and serving before its parent dies
      wr.puts(helper_actor.instance_variable_get(:@pid))
      wr.puts(helper_actor.instance_variable_get(:@socket_path))
      wr.close
      sleep 30
    end
    wr.close
    actor_pid = rd.gets.to_i
    socket_path = rd.gets.chomp
    rd.close

    Process.kill('KILL', helper_pid)
    Process.wait(helper_pid)

    # Sent well within the watchdog's 10 second poll, so it is this request
    # itself, not the watchdog, that makes the actor refuse and exit.
    conn = UNIXSocket.new(socket_path)
    conn.puts(JSON.generate({ 'method' => 'greet', 'args' => ['!'] }))

    # Closing a connection with our request still unread makes the kernel
    # reset it, so "no reply" can show up as either EOF or ECONNRESET.
    reply = begin
      conn.gets
    rescue Errno::ECONNRESET
      nil
    end
    expect(reply).to be_nil
    conn.close
    expect(wait_until_dead(actor_pid, within: 3)).to be true
  end
end
