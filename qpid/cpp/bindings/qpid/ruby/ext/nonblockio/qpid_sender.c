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

static VALUE qpid_wait_on_send(VALUE* args)
{
  VALUE adaptor = args[0];
  VALUE fd      = args[1];

  // start the handler
  rb_funcall(adaptor, rb_intern("start"), 0);

  rb_thread_wait_fd(FIX2INT(fd));

  return Qnil;
}


#ifdef RUBY19
static VALUE qpid_threaded_send(void* void_args)
{
  VALUE* args         = (VALUE*)void_args;
  VALUE  sender       = args[0];
  VALUE  message      = args[1];
  VALUE  sync         = args[2];
  VALUE  sender_impl  = rb_funcall(sender, rb_intern("sender_impl"), 0);
  VALUE  message_impl = rb_funcall(message, rb_intern("message_impl"), 0);

  rb_funcall(sender_impl, rb_intern("send"), 2, message_impl, sync);

  return message;
}
#endif


VALUE qpid_sender_send(int argc, VALUE* argv, VALUE self)
{
  VALUE sender = self;

  if(argc < 1 || argc > 2)
    {
      rb_raise(rb_eArgError, "A message must be specified.");
    }

  VALUE message = argv[0];
  VALUE result  = message;
  VALUE options;

  if(argc == 2)
    {
      options = argv[1];
    }
  else
    {
      options = rb_hash_new();
    }

  VALUE sync = rb_hash_aref(options, ID2SYM(rb_intern("sync")));
  VALUE send_object;

  if(sync == Qnil) sync = Qfalse;

#ifdef RUBY18
  send_object = rb_funcall(mSynchio, rb_intern("create_send_command"),
                           2, sender, message);

  VALUE success = qpid_wait_on_command(send_object);
#else
  VALUE args[3];
  VALUE success = Qtrue;

  args[0] = sender;
  args[1] = message;
  args[2] = sync;

  rb_thread_blocking_region(qpid_threaded_send, &args,
                            RUBY_UBF_PROCESS, 0);
#endif

  if(rb_block_given_p())
    {
      rb_yield(RTEST(success) ? message : Qnil);
    }

  return result;
}

