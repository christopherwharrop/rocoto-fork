# frozen_string_literal: true

require 'workflowmgr/utilities'

# Fixture that explains itself and then fails to start, the way a database
# that cannot reach its file would. The explanation is the whole point: it
# has to reach the caller along with the failure.
class NoisyStartupActorTestDouble
  def initialize
    WorkflowMgr.stderr('could not reach the database file', 1)
    raise ArgumentError, 'gave up while starting'
  end

  def greet
    'never reached'
  end
end
