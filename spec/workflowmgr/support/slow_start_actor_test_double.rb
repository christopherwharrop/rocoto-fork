# frozen_string_literal: true

# Fixture that wedges in its constructor, standing in for a served object
# whose setup touches a hung filesystem. An actor running this never reaches
# the point of accepting connections, which is what makes it the awkward
# case for shutting one down that was never called.
class SlowStartActorTestDouble
  def initialize
    sleep 120
  end

  def greet(suffix = "")
    "never reached#{suffix}"
  end
end
