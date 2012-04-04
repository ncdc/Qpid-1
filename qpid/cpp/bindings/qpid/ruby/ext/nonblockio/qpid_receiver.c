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


static VALUE qpid_receive(VALUE receiver, VALUE timeout,
                          char* method, char* utils_method)
{
  VALUE receive_object;
  VALUE result = Qnil;

  // create the receive action
  receive_object   = rb_funcall(mSynchio, rb_intern(utils_method),
                                2, receiver, timeout);

  VALUE success = qpid_wait_on_command(receive_object);

  if(RTEST(success))
    {
      VALUE message       = rb_funcall(receive_object,
                                       rb_intern("getMessage"), 0);

      // if we have a message then we must rewrap it
      if(RTEST(message))
        {
          ID    message_class_id;
          VALUE message_class;
          VALUE message_args[1];

          message_class_id = rb_intern("Message");
          message_class    = rb_const_get(mMessaging, message_class_id);
          message_args[0]  = rb_hash_new();
          rb_hash_aset(message_args[0], ID2SYM(rb_intern("impl")), message);
          result           = rb_class_new_instance(1, message_args,
                                                   message_class);
        }
    }

  // if we did not receive a message, then raise an error
  if(result == Qnil)
    {
      rb_raise(eMessagingError, "No message to fetch");
    }

  if(rb_block_given_p())
    {
      rb_yield(result);
    }

  return result;
}


VALUE qpid_ruby19_threaded_receive(void* void_args)
{
  VALUE* args     = (void*)void_args;
  VALUE  result   = Qnil;
  VALUE  receiver = args[0];
  VALUE  timeout  = args[1];
  VALUE  method   = args[2];

  VALUE duration_impl = rb_funcall(timeout, rb_intern("duration_impl"), 0);
  VALUE receiver_impl = rb_funcall(receiver, rb_intern("receiver_impl"), 0);

  VALUE receiver_args[1];

  receiver_args[0] = duration_impl;
  VALUE message    = rb_funcall(receiver_impl, method, 0); // 1, receiver_args);

  if(RTEST(message))
    {
      ID    message_class_id;
      VALUE message_class;
      VALUE message_args[1];

      message_class_id = rb_intern("Message");
      message_class    = rb_const_get(mMessaging, message_class_id);
      message_args[0]  = rb_hash_new();
      rb_hash_aset(message_args[0], ID2SYM(rb_intern("impl")), message);
      result           = rb_class_new_instance(1, message_args,
                                               message_class);
    }

  return result;
}


void qpid_ruby19_threaded_receive_interrupt()
{
  printf("THREAD INTERRUPT CALLED!\n");
}


static VALUE qpid_receiver_get_or_fetch(int argc, VALUE* argv, VALUE self,
                                        char* method, char* utils_method)
{
  VALUE result;
  VALUE timeout = Qnil;

  if(argc > 1)
    {
      rb_raise(rb_eArgError, "wrong number of arguments");
    }

  // if supplied, scan the remaining arguments
  if(argc == 1)
    {
      timeout = argv[0];
    }

  if(!RTEST(timeout))
    {
      timeout = qpid_get_duration_by_name(ID2SYM(rb_intern("FOREVER")));
    }

  // result = qpid_receive(self, timeout, method, utils_method);
  VALUE args[4];

  args[0] = self;
  args[1] = timeout;
  args[2] = rb_intern(method);
  args[3] = rb_intern(utils_method);

  result = rb_thread_blocking_region(qpid_ruby19_threaded_receive, &args,
                                     qpid_ruby19_threaded_receive_interrupt, 4);

  return result;
}


/* Overrides the constructor from the Ruby layer in order to
   set a default capacity of 1 for a Receiver. Otherwise it
   defaults to 0 and non-blocking I/O fails.
*/
VALUE qpid_receiver_initialize(VALUE self, VALUE session, VALUE receiver_impl)
{
  rb_ivar_set(self, rb_intern("@session"), session);
  rb_ivar_set(self, rb_intern("@receiver_impl"), receiver_impl);
  // set a default capacity
  rb_funcall(receiver_impl, rb_intern("setCapacity"), 1, INT2FIX(1));

  return self;
}


VALUE qpid_receiver_fetch(int argc, VALUE* argv, VALUE self)
{
  return qpid_receiver_get_or_fetch(argc, argv, self,
                                    "fetch", "create_receiver_fetch_command");
}


VALUE qpid_receiver_get(int argc, VALUE* argv, VALUE self)
{
  return qpid_receiver_get_or_fetch(argc, argv, self,
                                    "get", "create_receiver_get_command");
}
