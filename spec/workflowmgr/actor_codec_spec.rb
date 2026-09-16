# frozen_string_literal: true

require 'spec_helper'
require 'json'
require 'workflowmgr/actor'
require 'workflowmgr/job'
require 'workflowmgr/cycle'
require_relative 'support/codec_actor_test_double'

RSpec.describe WorkflowMgr::Actor::Codec do
  # Exactly what the transport does to a value: encode it, serialize it,
  # parse it back, decode it.
  def round_trip(value)
    described_class.decode(JSON.parse(JSON.generate(described_class.encode(value))))
  end

  it 'carries symbols, which JSON has no notion of' do
    expect(round_trip(:running)).to eq(:running)
    expect(round_trip([:a, 'a'])).to eq([:a, 'a'])
  end

  it 'carries times without losing precision or whether they were UTC' do
    time = Time.at(1_700_000_000, 123_456_789, :nanosecond).utc
    carried = round_trip(time)

    expect(carried).to eq(time)
    expect(carried.nsec).to eq(123_456_789)
    expect(carried.utc?).to be true
  end

  it 'carries hashes keyed by symbols and by times' do
    cycle = Time.at(1_700_000_000).utc

    expect(round_trip({ start: cycle, end: nil })).to eq({ start: cycle, end: nil })
    expect(round_trip({ cycle => 'a cycle' })).to eq({ cycle => 'a cycle' })
  end

  it 'leaves an ordinary string-keyed hash as it found it' do
    plain = { 'a' => 1, 'b' => [2, nil, true, 1.5] }
    expect(round_trip(plain)).to eq(plain)
  end

  it 'does not mistake data that happens to look like a tag for one' do
    lookalike = { '$' => 'Time', 'v' => [1, 2, true] }
    expect(round_trip(lookalike)).to eq(lookalike)
  end

  it 'carries a Job by the fields it declares' do
    cycle = Time.at(1_700_000_000).utc
    job = WorkflowMgr::Job.new('12345', 'forecast', cycle, 8, 'RUNNING', 'R', 0, 1, 0, 12.5)
    carried = round_trip(job)

    expect(carried).to be_a(WorkflowMgr::Job)
    expect([carried.id, carried.task, carried.cycle, carried.cores]).to eq(['12345', 'forecast', cycle, 8])
    expect([carried.state, carried.native_state, carried.tries, carried.duration]).to eq(['RUNNING', 'R', 1, 12.5])
  end

  it 'carries a Cycle, letting its constructor derive the state as always' do
    cycle_time = Time.at(1_700_000_000).utc
    activated = Time.at(1_700_000_100).utc
    zero = Time.at(0).utc
    cycle = WorkflowMgr::Cycle.new(cycle_time,
                                   { activated: activated, expired: zero, done: zero, draining: zero })
    carried = round_trip(cycle)

    expect(carried.cycle).to eq(cycle_time)
    expect(carried.activated).to eq(activated)
    expect(carried.state).to eq(:active)
  end

  it 'carries the shape load_jobs returns: tasks, cycle times and Jobs nested together' do
    cycle = Time.at(1_700_000_000).utc
    job = WorkflowMgr::Job.new('7', 'post', cycle, 1, 'QUEUED', 'Q', 0, 0, 0, 0.0)
    carried = round_trip({ 'post' => { cycle => job } })

    expect(carried['post'].keys).to eq([cycle])
    expect(carried['post'][cycle].id).to eq('7')
  end

  it 'refuses a value it cannot carry rather than sending its to_s' do
    expect { described_class.encode(Object.new) }
      .to raise_error(described_class::Unsupported, /cannot be carried/)
  end

  it 'refuses to rebuild a class that never opted in' do
    expect { described_class.decode({ '$' => 'Kernel', 'v' => [] }) }
      .to raise_error(described_class::Unsupported, /from_wire/)
    expect { described_class.decode({ '$' => 'NoSuchClassAnywhere', 'v' => [] }) }
      .to raise_error(described_class::Unsupported)
  end

  it 'refuses a structure nested deeper than it can carry, naming the real limit' do
    deep = (1..40).reduce([]) { |inner, _| [inner] }
    expect { described_class.encode(deep) }
      .to raise_error(described_class::Unsupported, /nested deeper/)
  end

  it 'refuses a structure that contains itself, rather than recursing until the stack gives out' do
    looping = []
    looping << looping

    expect { described_class.encode(looping) }.to raise_error(described_class::Unsupported)
  end

  it 'keeps the wall-clock reading of a time that was not UTC' do
    time = Time.at(1_700_000_000).localtime('-06:00')
    carried = round_trip(time)

    # Equality alone would not catch this: it compares instants, while a
    # Cycle prints itself with strftime, which does not.
    expect(carried).to eq(time)
    expect(carried.strftime('%Y%m%d%H%M')).to eq(time.strftime('%Y%m%d%H%M'))
    expect(carried.utc_offset).to eq(time.utc_offset)
  end

  it 'refuses floats JSON has no way to write, where they were handed over' do
    expect { described_class.encode(Float::NAN) }
      .to raise_error(described_class::Unsupported, /NaN/)
    expect { described_class.encode(Float::INFINITY) }
      .to raise_error(described_class::Unsupported)
  end

  it 'refuses a string holding bytes that are not valid UTF-8' do
    expect { described_class.encode("abc\xC3\x28".dup.force_encoding('UTF-8')) }
      .to raise_error(described_class::Unsupported, /not valid UTF-8/)
  end

  it 'refuses a malformed hash tag instead of failing obscurely' do
    expect { described_class.decode({ '$' => 'Hash', 'v' => 'not pairs' }) }
      .to raise_error(described_class::Unsupported, /pairs/)
  end

  describe 'across an actor boundary' do
    it 'delivers a range given with symbol keys, instead of quietly losing it' do
      actor = WorkflowMgr::Actor.spawn(CodecActorTestDouble, timeout: 10)
      first = Time.at(1_700_000_000).utc
      last = Time.at(1_700_003_600).utc

      # Without the codec both keys arrive as strings, reftime[:start] reads
      # as nil, and the real load_cycles falls back to every cycle ever.
      expect(actor.cycle_range({ start: first, end: last })).to eq([first, last])
    ensure
      actor&.stop!
    end

    it 'delivers Job objects as Jobs, not as their inspect output' do
      actor = WorkflowMgr::Actor.spawn(CodecActorTestDouble, timeout: 10)
      cycle = Time.at(1_700_000_000).utc
      job = WorkflowMgr::Job.new('999', 'post', cycle, 4, 'SUBMITTING', 'Q', 0, 0, 0, 0.0)

      expect(actor.job_seen(job)).to eq(['WorkflowMgr::Job', '999', 'post', cycle, 'SUBMITTING', 0.0])
    ensure
      actor&.stop!
    end

    it 'carries constructor arguments as well as method arguments' do
      actor = WorkflowMgr::Actor.spawn(CodecActorTestDouble, { mode: :fast }, timeout: 10)
      expect(actor.options_seen).to eq([[:mode], :fast])
    ensure
      actor&.stop!
    end

    it 'brings symbols and times back as themselves' do
      actor = WorkflowMgr::Actor.spawn(CodecActorTestDouble, timeout: 10)

      expect(actor.echo(:submitted)).to eq(:submitted)
      expect(actor.echo(Time.at(0).utc)).to eq(Time.at(0).utc)
      expect(actor.class_of(Time.at(0).utc)).to eq('Time')
    ensure
      actor&.stop!
    end

    it 'refuses an uncarryable argument without disturbing the actor' do
      actor = WorkflowMgr::Actor.spawn(CodecActorTestDouble, timeout: 10)

      expect { actor.echo(Object.new) }.to raise_error(WorkflowMgr::Actor::Codec::Unsupported)
      expect(actor.echo(:still_here)).to eq(:still_here)
    ensure
      actor&.stop!
    end

    it 'carries encoded payloads across the socket buffer boundary intact' do
      actor = WorkflowMgr::Actor.spawn(CodecActorTestDouble, timeout: 30)

      # Sizes either side of the point where a payload stops fitting in one
      # write, with content that is multibyte and full of JSON escapes.
      [4_096, 65_536, 200_001, 500_000].each do |size|
        payload = "café \"\\\n\t" * ((size / 12) + 1)
        expect(actor.echo(payload)).to eq(payload)
        expect(actor.echo({ payload.to_sym => [payload] })).to eq({ payload.to_sym => [payload] })
      end
    ensure
      actor&.stop!
    end
  end
end
