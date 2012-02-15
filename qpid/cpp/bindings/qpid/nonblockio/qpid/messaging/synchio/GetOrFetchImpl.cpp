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

#include "qpid/messaging/synchio/GetOrFetchImpl.h"

using namespace qpid::messaging;

namespace qpid
{

  namespace messaging
  {

    namespace synchio
    {

      GetOrFetchImpl::GetOrFetchImpl(GetOrFetchMode mode,
                                     Receiver& receiver,
                                     Duration& timeout):
        BaseThreadedEventHandler(),
        mode(mode),
        receiver(receiver),
        timeout(timeout)
      { }


      GetOrFetchImpl::~GetOrFetchImpl()
      { }


      void GetOrFetchImpl::run()
      {
        bool success;

        if(mode == GET)
          {
            success = receiver.get(message, timeout);
          }
        else
          {
            success = receiver.fetch(message, timeout);
          }

        notifyListeners(success);
      }


      Message GetOrFetchImpl::getMessage()
      {
        return message;
      }


    }

  }

}