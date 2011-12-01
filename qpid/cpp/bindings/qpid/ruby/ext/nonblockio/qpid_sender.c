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

  send_object = rb_funcall(mSynchio, rb_intern("create_send_command"),
                           2, sender, message);

  VALUE success = qpid_wait_on_command(send_object);

  if(rb_block_given_p())
    {
      rb_yield(RTEST(success) ? message : Qnil);
    }

  return result;
}

