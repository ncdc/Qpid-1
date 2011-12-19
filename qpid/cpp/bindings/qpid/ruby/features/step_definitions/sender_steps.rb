Given /^the message "([^"]*)" is sent$/ do |content|
  @sender.send Qpid::Messaging::Message.new :content => "#{content}"
end

Then /^sending the message "([^"]*)" should raise an error$/ do |content|
  lambda {
    steps %Q{
      @sender.send Qpid::messaging::Message.new :content => "#{content}"
    }
  }.should raise_error
end

Then /^sending the message "([^"]*)" succeeds$/ do |content|
  @sender.send Qpid::Messaging::Message.new :content => "#{content}"
end
