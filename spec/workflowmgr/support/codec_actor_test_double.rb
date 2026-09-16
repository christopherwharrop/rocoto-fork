# frozen_string_literal: true

# An actor process loads only workflowmgr/actor and this file, so anything
# this class expects to be handed, or hands back, has to be required here.
# The real served classes do the same: workflowdb.rb requires job and cycle.
require 'workflowmgr/job'
require 'workflowmgr/cycle'

# Fixture for the codec's actor-boundary specs. Its methods mirror the
# shapes the real database uses: an options hash with symbol keys, a range
# given as {start:, end:}, and arrays of Job objects.
class CodecActorTestDouble
  def initialize(options = { mode: :plain })
    @options = options
  end

  # Reports the options it was constructed with, to check that constructor
  # arguments survive the trip as well as method arguments do.
  def options_seen
    [@options.keys, @options[:mode]]
  end

  def echo(value)
    value
  end

  # Mirrors WorkflowSQLite3DB#load_cycles. The failure this guards against
  # is reftime[:start] arriving as nil, whereupon the real method falls back
  # to its default range and returns every cycle in the database.
  def cycle_range(reftime = { start: Time.at(0), end: Time.at(0) })
    [reftime[:start], reftime[:end]]
  end

  # Reports what a Job actually looked like on this side.
  def job_seen(job)
    [job.class.name, job.id, job.task, job.cycle, job.state, job.duration]
  end

  def class_of(value)
    value.class.name
  end
end
