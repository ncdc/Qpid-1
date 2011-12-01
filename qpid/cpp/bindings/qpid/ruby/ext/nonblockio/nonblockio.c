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

#include "nonblockio.h"

VALUE mCqpid;
VALUE mQpid;
VALUE mMessaging;
VALUE mSynchio;
VALUE cDuration;
VALUE cReceiver;
VALUE cSender;
VALUE cSession;
VALUE eMessagingError;


void Init_nonblockio()
{
  mCqpid     = rb_const_get(rb_cObject, rb_intern("Cqpid"));
  mQpid      = rb_define_module("Qpid");
  mMessaging = rb_define_module_under(mQpid, "Messaging");
  mSynchio   = rb_define_module_under(mMessaging, "Synchio");

  cDuration  = rb_define_class_under(mMessaging, "Duration", rb_cObject);

  cReceiver  = rb_define_class_under(mMessaging, "Receiver", rb_cObject);
  rb_define_method(cReceiver, "initialize", qpid_receiver_initialize, 2);
  rb_define_method(cReceiver, "fetch", qpid_receiver_fetch, -1);
  rb_define_method(cReceiver, "get", qpid_receiver_get, -1);

  cSender = rb_define_class_under(mMessaging, "Sender", rb_cObject);
  rb_define_method(cSender, "send", qpid_sender_send, -1);

  cSession = rb_define_class_under(mMessaging, "Session", rb_cObject);
  rb_define_method(cSession, "acknowledge_with_sync",
                   qpid_session_acknowledge_with_synch, 1);
  rb_define_method(cSession, "next_receiver", qpid_session_next_receiver, -1);
  rb_define_method(cSession, "sync_and_block", qpid_session_sync_and_block, 0);

  eMessagingError = rb_define_class("MessagingError", rb_eStandardError);
}
