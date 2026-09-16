# frozen_string_literal: true

# Fixture whose *file* can be made slow to load, or made to fail while
# loading, so actor_spec.rb can exercise what happens before an actor has
# finished starting up -- the window in which it cannot serve anything yet.
#
# The spec process loads this file with neither variable set, so it is
# instant and harmless there. Only an actor process spawned while one of
# them is set sees the behaviour, since the environment is inherited.
sleep Integer(ENV["ROCOTO_SPEC_SLOW_LOAD"]) if ENV["ROCOTO_SPEC_SLOW_LOAD"]
raise ENV["ROCOTO_SPEC_FAIL_LOAD"] if ENV["ROCOTO_SPEC_FAIL_LOAD"]

class LoadHooksActorTestDouble
  def greet(suffix = "")
    "hello#{suffix}"
  end
end
