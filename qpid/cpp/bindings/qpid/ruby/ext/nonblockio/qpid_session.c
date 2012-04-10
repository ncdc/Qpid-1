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

#include <sys/types.h>

#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>

#define IN_BUFFER_SIZE 4096
#define ERROR_MESSAGE_LEN 1024

#ifdef RUBY18
static VALUE qpid_wait_on_next_receiver(VALUE* args)
{
  VALUE next_receiver_object = args[0];
  VALUE next_receiver_fd     = args[1];
  int   fd;

  // start the handler
  rb_funcall(next_receiver_object, rb_intern("start"), 0);

  fd = FIX2INT(next_receiver_fd);

  rb_thread_wait_fd(fd);

  return Qnil;
}
#endif


VALUE qpid_session_acknowledge_with_synch(VALUE self, VALUE message)
{
  VALUE session = self;
  VALUE result  = Qnil;
  VALUE acknowledge;

  acknowledge = rb_funcall(mSynchio, rb_intern("create_acknowledge_command"),
                           2, session, message);

  result = qpid_wait_on_command(acknowledge);

  return result;
}


#ifdef RUBY19
static VALUE qpid_threaded_next_receiver(void* void_args)
{
  VALUE* args         = (VALUE*)void_args;
  VALUE self          = args[0];
  VALUE timeout       = args[1];
  VALUE duration_impl = rb_funcall(timeout, rb_intern("duration_impl"), 0);
  VALUE session_impl  = rb_funcall(self, rb_intern("session_impl"), 0);
  VALUE result        = rb_funcall(session_impl, rb_intern("nextReceiver"),
                                   1, duration_impl);

  return result;
}
#endif


VALUE qpid_session_next_receiver(int argc, VALUE* argv, VALUE self)
{
  VALUE timeout;
  VALUE session = self;
  VALUE next_receiver;
  VALUE result = Qnil;

  if(argc == 1)
    {
      timeout = argv[0];
    }
  else
    {
      timeout = qpid_get_duration_by_name(ID2SYM(rb_intern("FOREVER")));
    }

  VALUE receiver_impl = Qnil;

#ifdef RUBY18
  next_receiver = rb_funcall(mSynchio, rb_intern("create_next_receiver_command"),
                             2, session, timeout);

  VALUE success = qpid_wait_on_command(next_receiver);

  if(RTEST(success))
    {
      receiver_impl = rb_funcall(next_receiver, rb_intern("getReceiver"), 0);
    }

#else
  VALUE args[2];

  args[0] = self;
  args[1] = timeout;

  receiver_impl = rb_thread_blocking_region(qpid_threaded_next_receiver, &args,
                                            RUBY_UBF_PROCESS, NULL);
#endif

  if(RTEST(receiver_impl))
    {
      ID    receiver_class_id;
      VALUE receiver_class;
      VALUE receiver_args[2];

      receiver_class_id = rb_intern("Receiver");
      receiver_class    = rb_const_get(mMessaging, receiver_class_id);
      receiver_args[0]  = self;
      receiver_args[1]  = receiver_impl;
      result           = rb_class_new_instance(2, receiver_args, receiver_class);
    }

  if(rb_block_given_p())
    {
      rb_yield(result);
    }

  return result;
}

#ifdef RUBY19
static VALUE qpid_threaded_sync_and_block(void* void_args)
{
  VALUE* args        = (VALUE*)void_args;
  VALUE self         = args[0];
  VALUE session_impl = rb_funcall(self, rb_intern("session_impl"), 0);

  rb_funcall(session_impl, rb_intern("sync"), 1, Qtrue);

  return Qnil;
}
#endif

VALUE qpid_session_sync_and_block(VALUE self)
{
  VALUE session = self;

#ifdef RUBY18
  VALUE sync = rb_funcall(mSynchio, rb_intern("create_sync_command"),
                          1, session);

  qpid_wait_on_command(sync);
#else
  VALUE args[1];

  args[1] = self;
  rb_thread_blocking_region(qpid_threaded_sync_and_block, &args,
                            RUBY_UBF_PROCESS, NULL);
#endif

  return Qnil;
}
