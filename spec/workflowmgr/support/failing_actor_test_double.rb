# frozen_string_literal: true

# Fixture whose constructor always fails, used by actor_spec.rb to exercise
# what a caller is told when an actor can't start at all.
class FailingActorTestDouble
  def initialize(_name)
    raise ArgumentError, "cannot be constructed"
  end

  def greet(suffix = "")
    "never reached#{suffix}"
  end
end
