# frozen_string_literal: true

# Tiny fixture class used only by actor_spec.rb to exercise WorkflowMgr::Actor
# without depending on any real, more complex served object.
class EchoActorTestDouble
  def initialize(name)
    @name = name
  end

  def greet(suffix = "")
    "hello #{@name}#{suffix}"
  end

  def boom
    raise ArgumentError, "kaboom"
  end

  def nap(seconds)
    sleep(seconds)
    "awake"
  end

  # Raises an Errno error of its own, which must reach the caller as that
  # error rather than being mistaken for the actor itself failing.
  def read_missing_file
    File.read("/nonexistent/rocoto-actor-spec")
  end

  # A programming error rather than a runtime one: LoadError is not a
  # StandardError, so it stands in for broken code that must not keep
  # serving, but whose cause the caller still needs to be told.
  def bad_require
    require "no_such_library_for_rocoto_spec"
  end

  # Ends the actor process in the middle of a request.
  def die
    exit(1)
  end
end
