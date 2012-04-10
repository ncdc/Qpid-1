#ifndef QPID_EXT_NONBLOCKIO_H
#define QPID_EXT_NONBLOCKIO_H

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * To create the Makefile then you need to specify the location
 * of the Qpid shared libraries using the commandline:
 *
 */

#include <ruby.h>

#ifdef RUBY18
#include <rubyio.h>
#else
#include <ruby/io.h>
#endif

extern VALUE mCqpid;
extern VALUE mQpid;
extern VALUE mMessaging;
extern VALUE mSynchio;
extern VALUE cDuration;
extern VALUE cReceiver;
extern VALUE cSender;
extern VALUE cSession;
extern VALUE eMessagingError;


// Utility methods
VALUE qpid_is_main_thread();
VALUE qpid_wait_on_command(VALUE command);

// Qpid::Messaging::Duration
VALUE qpid_get_duration_by_name(VALUE name);

// Qpid::Messaging::Receiver
VALUE qpid_receiver_initialize(VALUE self, VALUE session, VALUE receiver_impl);
VALUE qpid_receiver_fetch(int argc, VALUE* argv, VALUE self);
VALUE qpid_receiver_get(int argc, VALUE* argv, VALUE self);

// Qpid::Messaging::Sender
VALUE qpid_sender_send(int argc, VALUE* argv, VALUE self);

// Qpid::Messaging::Session
VALUE qpid_session_acknowledge_with_synch(VALUE self, VALUE message);
VALUE qpid_session_next_receiver(int argc, VALUE* argv, VALUE self);
VALUE qpid_session_sync_and_block(VALUE self);

#endif
