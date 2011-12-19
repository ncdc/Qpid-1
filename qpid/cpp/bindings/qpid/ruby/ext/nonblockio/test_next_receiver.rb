#!/usr/bin/env ruby

require 'rubygems'
require 'qpid'
require 'mathn'

MAX_RECEIVERS=ARGV.length > 0 ? ARGV[0].to_i : 50

QUIT_KEY = (0...16).map{ ('a'..'z').to_a[rand(26)] }.join

# create a connection, session and a sender
conn      = Qpid::Messaging::Connection.new
conn.open
session   = conn.create_session
sender    = session.create_sender "my-queue;{create:always}"
receivers = []

# create a whole bunch of receivers for specific topics
(1..MAX_RECEIVERS).each do |which|
  puts "Creating receiver ##{which}..."
  recv = session.create_receiver "my-queue"
  receivers << recv
end

finished = false
received = 0
sent     = 0

# spin up a thread for receiver messages
receive_thread = Thread.new do
  loop do
    break if finished
    begin
      started = Time.new
      print "Waiting on the next receiver.\n"
      receiver = session.next_receiver
      if receiver.nil?
        print "No receiver received.\n"
      else
        print "Got a receiver: #{receiver}\n"
        print "Getting the pending message...\n"
        msg = receiver.get Qpid::Messaging::Duration::IMMEDIATE
        if msg.nil?
          print "NO MESSAGE? WTF?\n"
        else
          received += 1
          print "Message content: #{msg.content}\n"
          session.acknowledge :message => msg
          finished = true if msg.content =~ /#{QUIT_KEY}/
        end
        ended = Time.new
        print "Message receive cycle took #{ended - started} seconds.\n"
      end
    rescue Exception => error
      print "ERROR: #{error}\n"
      error.backtrace.each {|line| print "#{line}\n"}
      break
    end
  end
end


prime_thread = Thread.new do
  iterations = 0
  prime = Prime.new
  loop do
    break if finished

    next_prime = prime.next
    iterations += 1
    print "#{iterations} total iterations on primes (current=#{next_prime})\n" if (iterations % 100).zero?
  end
end


print "Sending messages to the various topics.\n"
(1..MAX_RECEIVERS).each do |which|
  sent += 1
  print "Sending message ##{which}.\n"
  msg = Qpid::Messaging::Message.new :content => "This is message ##{which}.#{which == MAX_RECEIVERS ? QUIT_KEY : ''}"
  sender.send msg
end

receive_thread.join
prime_thread.join

print "Shutting down connection.\n"
conn.close

puts "Sent #{sent} messages, received #{received} messages."
