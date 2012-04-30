#!/usr/bin/env ruby

require 'rubygems'
require 'qpid'
require 'nonblockio'
require 'mathn'

MAX_ITERATIONS = ARGV.length > 0 ? ARGV[0].to_i : 100
SEND_ONLY = (ARGV.length > 1 && ARGV[1] == "S")
RECV_ONLY = (ARGV.length > 0 && ARGV[0] == "R")

unless RECV_ONLY
  QUIT_KEY = (0...16).map{ ('a'..'z').to_a[rand(26)] }.join

  puts "Sending #{MAX_ITERATIONS} iterations, waiting for '#{QUIT_KEY}'..."
end

connection = Qpid::Messaging::Connection.new :url => "localhost", :reconnect_limit => 50
connection.open

session = connection.create_session
snd = session.create_sender "my-queue;{create:always}"
snd.capacity = 1000
rcv = session.create_receiver "my-queue"
rcv.capacity = 1000

finished = false
restart  = false
received = 0

started = Time.new

unless SEND_ONLY
  thread_receiving = Thread.new do
    begin
      loop do
        break if finished
        message = rcv.get Qpid::Messaging::Duration::SECOND
          if message.nil?
            printf "We got a nil message!\n"
            break
          else
            printf "Received and acknowledging: #{message.content}\n"
            rcv.session.acknowledge if (received % 100).zero?
            received += 1
            # finished = true if message.content =~ /#{QUIT_KEY}/
          end
        # end
      end
    rescue Exception => error
      print "#{error}\n"
      error.backtrace.each {|line| print "#{line}\n"}
    end
  end
end

sent = 0

unless RECV_ONLY
  iterations = 0
  loop do
    break if finished

    if (iterations < MAX_ITERATIONS)
      iterations += 1
      #    (1..MAX_MESSAGES).each do |which|
      sent += 1
      before = Time.new
      msg = Qpid::Messaging::Message.new :content => "This is a test (##{sent}) (#{Time.new})#{iterations == MAX_ITERATIONS ? ' ' + QUIT_KEY : ''}."

      printf "Sending #{msg.content}\n"
      snd.send msg

      after = Time.new

      restart = true
      break if iterations == MAX_ITERATIONS
    end
  end
end

thread_receiving.join unless SEND_ONLY

finished = Time.new

snd.session.acknowledge

connection.close

puts "Sent #{sent} messages."
puts "Received #{received} messages."
puts "Runtime: from #{started} to #{finished} (#{finished-started} seconds)"
