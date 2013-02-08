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
 */

#include "ruby.h"

VALUE mQpid;
VALUE mMessaging;
VALUE cSessionDispatcher;

static VALUE session_dispatcher_initialize(VALUE self, VALUE session)
{
  rb_iv_set(self, "@session", session);
  rb_iv_set(self, "@running", Qfalse);

  return self;
}

static VALUE session_wait_for_receiver_nogvl(void *vargs)
{
  VALUE* args    = (VALUE*)vargs;
  VALUE  session = args[0];

  printf("Calling next receiver and waiting.\n");
  args[1] = rb_funcall(session, rb_intern("next_receiver"), 0);
  printf("Got our next receiver.\n");

  return Qnil;
}

static VALUE session_dispatcher_thread(void* vargs)
{
  VALUE self    = ((VALUE*)vargs)[0];
  VALUE session = rb_iv_get(self, "@session");
  VALUE args[2];

  args[0] = session;
  args[1] = Qnil; // will hold the receiver

  printf("Running = %s\n", RTEST(rb_iv_get(self, "@running")) ? "YES" : "NO");

  while(RTEST(rb_iv_get(self, "@running")))
    {
      printf("Starting the thread blocking region.\n");
      rb_thread_blocking_region(session_wait_for_receiver_nogvl, &args,
                                RUBY_UBF_PROCESS, NULL);
      rb_funcall(session, rb_intern("incoming"), 1, args[1]);
      printf("Continue running? %s\n", RTEST(rb_iv_get(self, "@running")) ? "YES" : "NO");
    }

  return Qnil;
}

static VALUE session_dispatcher_start(VALUE self)
{
  // if we're already running then exit
  VALUE running = rb_iv_get(self, "@running");
  printf("Are we already running? %s\n", RTEST(running) ? "YES" : "NO");

  if(!RTEST(running))
    {
      VALUE args[1];

      args[0] = self;

      printf("Setting the running flag.\n");
      rb_iv_set(self, "@running", Qtrue);

      VALUE thread = rb_thread_create(session_dispatcher_thread, &args);
      rb_funcall(thread, rb_intern("join"), 0);
    }

  return Qnil;
}

static VALUE session_dispatcher_stop(VALUE self)
{
  rb_iv_set(self, "@running", Qfalse);
  return Qnil;
}

void Init_qpid_session_dispatcher()
{
  mQpid = rb_define_module("Qpid");
  mMessaging = rb_define_module_under(mQpid, "Messaging");
  cSessionDispatcher = rb_define_class_under(mMessaging,
                                             "SessionDispatcher", rb_cObject);

  rb_define_method(cSessionDispatcher, "initialize",
                   session_dispatcher_initialize, 1);
  rb_define_method(cSessionDispatcher, "start",
                   session_dispatcher_start, 0);
  rb_define_method(cSessionDispatcher, "stop",
                   session_dispatcher_stop, 0);
}

