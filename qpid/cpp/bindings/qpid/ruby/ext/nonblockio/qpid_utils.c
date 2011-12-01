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
#include "nonblockio.h"

VALUE qpid_is_main_thread()
{
  VALUE current_thread = rb_thread_current();
  VALUE main_thread    = rb_thread_main();
  VALUE result         = current_thread == main_thread ? Qtrue : Qfalse;

  return result;
}


static VALUE qpid_thread_wait_method(VALUE* args)
{
  int   fd;
  VALUE command;

  command = args[0];
  fd      = FIX2INT(args[1]);

  rb_funcall(command, rb_intern("start"), 0);
  rb_thread_wait_fd(fd);

  return command;
}

VALUE qpid_wait_on_command(VALUE command)
{
  VALUE result;

  /* if we're running a test, then exit */
  if(!RTEST(rb_gv_get("$QPID_TESTING_ENVIRONMENT")))
    {
      VALUE command_args[2];

      command_args[0] = command;
      command_args[1] = rb_funcall(command, rb_intern("getHandle"), 0);

      /* if we're in t  he main thread then we need to spawn a new thread
       * to handle retrievin    g the message
       */
      if (RTEST(qpid_is_main_thread()))
        {
          VALUE receive_thread = rb_thread_create(qpid_thread_wait_method,
                                                  command_args);

          rb_funcall(receive_thread, rb_intern("join"), 0);
        }
      else
        {
          qpid_thread_wait_method(command_args);
        }

      // stop    the handler
      rb_funcall(command, rb_intern("stop"), 0);
    }

  result = rb_funcall(command, rb_intern("getSuccess"), 0);

  return result;
}


VALUE qpid_get_duration_by_name(VALUE name)
{
  VALUE durations;

  durations = rb_cvar_get(cDuration, rb_intern("@hash"));

  return rb_hash_aref(durations, name);
}
