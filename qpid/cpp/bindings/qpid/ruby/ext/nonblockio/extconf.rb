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

# To create the Makefile then you need to specify the location
# of the Qpid shared libraries using the commandline:
#
#  $ ruby extconf.rb --with-qpid-lib=[path to libraries]
#

require 'mkmf'

# specify the version of Ruby being used as a macro: RUBY##
runtime_version = RbConfig::CONFIG["ruby_version"].gsub(/\./, "")[0, 2]
using_ruby_18 = (runtime_version == "18")
$CPPFLAGS << " -DRUBY#{runtime_version}"

# tell Ruby to use a C++ compiler and to stop after preprocessing
RbConfig::CONFIG['CPP'] = "g++ -E"

dir_config('qpid')
dir_config('nonblockio') if using_ruby_18

def require_library(library)
  fail("Missing library: #{library}") unless have_library(library)
end

def require_header(header)
  fail("Missing header: #{header}") unless have_header(header)
end

# ensure the required libraries are present
require_library("qpidclient")
require_library("qpidcommon")
require_library("qpidmessaging")
require_library("qpidtypes")

if using_ruby_18
  require_library("qpidnonblockio")
end

# ensure the required headers are present
require_header("qpid/messaging/exceptions.h")
require_header("qpid/messaging/Address.h")
require_header("qpid/messaging/Connection.h")
require_header("qpid/messaging/Session.h")
require_header("qpid/messaging/Receiver.h")
require_header("qpid/messaging/Sender.h")
require_header("qpid/messaging/Message.h")
require_header("qpid/messaging/Duration.h")
require_header("qpid/messaging/FailoverUpdates.h")

#headers required by for Ruby 1.8
if using_ruby_18
#  require_header("qpid/messaging/synchio/Acknowledge.h")
#  require_header("qpid/messaging/synchio/NextReceiver.h")
#  require_header("qpid/messaging/synchio/ReceiverFetch.h")
#  require_header("qpid/messaging/synchio/ReceiverGet.h")
#  require_header("qpid/messaging/synchio/Send.h")
#  require_header("qpid/messaging/synchio/SessionSync.h")
end

create_makefile('nonblockio')

