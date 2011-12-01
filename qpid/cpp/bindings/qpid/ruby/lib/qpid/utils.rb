#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

require 'cqpid'

module Qpid

  module Messaging

    module Synchio # :nodoc:

      def self.create_acknowledge_command(session, message) # :nodoc:
        Cqpid::Acknowledge.new(session.session_impl,
                               message.nil? ? nil : message.message_impl)
      end

      def self.create_next_receiver_command(session, duration) # :nodoc:
        Cqpid::NextReceiver.new(session.session_impl, duration.duration_impl)
      end

      def self.create_receiver_fetch_command(receiver, duration) # :nodoc:
        Cqpid::ReceiverFetch.new(receiver.receiver_impl, duration.duration_impl)
      end

      def self.create_receiver_get_command(receiver, duration) # :nodoc:
        Cqpid::ReceiverGet.new(receiver.receiver_impl, duration.duration_impl)
      end

      def self.create_send_command(sender, message) # :nodoc:
        Cqpid::Send.new(sender.sender_impl,message.message_impl)
      end

      def self.create_sync_command(session) # :nodoc:
        Cqpid::SessionSync.new session.session_impl
      end

    end

  end

end
