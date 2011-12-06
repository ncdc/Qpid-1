/*
 *
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
 */

#include "qpid/messaging/Prong.h"
#include "qpid/messaging/exceptions.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

using namespace qpid::messaging;

namespace qpid
{

  namespace messaging
  {

    Prong::Prong()
      :myHandle(-1)
      ,yourHandle(-1)
    { open(); }

    Prong::~Prong() { close(); }

    void Prong::open()
    {
      int pair[2];

      if(::pipe(pair) == -1)
        {
          std::string message;

          switch(errno)
            {
            case EFAULT: message = "pipefd is not valid."; break;
            case EINVAL: message = "invalid value in flags"; break;
            case EMFILE: message = "too many file descriptors are in use"; break;
            case ENFILE: message = "system limit on open files reached"; break;
            }

          throw MessagingException(message);
        }

      yourHandle = pair[0];
      myHandle = pair[1];

      int flags;

      flags = ::fcntl(yourHandle, F_GETFL);
      if((::fcntl(yourHandle, F_SETFL, flags | O_NONBLOCK)) == -1)
        throw MessagingException("Unable to make your handle non-blocking.");

      flags = ::fcntl(myHandle, F_GETFL);
      if((::fcntl(myHandle, F_SETFL, flags | O_NONBLOCK)) == -1)
        throw MessagingException("Unable     to my handle non-blocking.");
    }

    void Prong::close()
    {
      if(myHandle > 0)
        ::close(myHandle);
      if(yourHandle > 0)
        ::close(yourHandle);

      myHandle = yourHandle = -1;
    }

    void Prong::updateHandles()
    {
      ::write(myHandle, "1", 1);
    }

    int Prong::getHandle()
    {
      return yourHandle;
    }

  }

}

