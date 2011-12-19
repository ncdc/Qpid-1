#!/usr/bin/env ruby

require 'rubygems'
require 'qpid'
require 'nonblockio'
require 'mathn'

MAX_ITERATIONS = ARGV.length > 0 ? ARGV[0].to_i : 100
MAX_MESSAGES   = ARGV.length > 1 ? ARGV[1].to_i : 5

QUIT_KEY = (0...16).map{ ('a'..'z').to_a[rand(26)] }.join

puts "Sending #{MAX_ITERATIONS} iterations, waiting for '#{QUIT_KEY}'..."

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

thread_receiving = Thread.new do
  begin
    loop do
      break if finished
      puts "Waiting for the next message."
      # Qpid::Messaging.receive rcv, :timeout => Qpid::Messaging::Duration::SECOND do |message|
      message = rcv.get Qpid::Messaging::Duration::SECOND
        if message.nil?
          puts "We got a nil message!"
          break
        else
          puts "Acknowledge the message."
          rcv.session.acknowledge if (received % 100).zero?
          puts "Message: #{message.content}"
          received += 1
          finished = true if message.content =~ /#{QUIT_KEY}/
          # sleep 0.0001
        end
      # end
    end
  rescue Exception => error
    print "#{error}\n"
    error.backtrace.each {|line| print "#{line}\n"}
    exit
  end

end

sent = 0
iterations = 0
loop do
  break if finished

  if (iterations < MAX_ITERATIONS)
    iterations += 1
    #    (1..MAX_MESSAGES).each do |which|
    sent += 1
    puts "Sending message #{sent}..."
    before = Time.new
    msg = Qpid::Messaging::Message.new :content => "This is a test (##{sent}) (#{Time.new})#{iterations == MAX_ITERATIONS ? ' ' + QUIT_KEY : ''}."

    snd.send msg

    after = Time.new
    puts "Messaging sent in #{after - before} seconds."
    #      sleep(0.1)
    #end

    restart = true
    break if iterations == MAX_ITERATIONS

    # puts "Sleeping for 5 seconds"
    # sleep(3)
  end

end

thread_receiving.join

snd.session.acknowledge

connection.close

puts "Sent #{sent} messages."
puts "Received #{received} messages."

