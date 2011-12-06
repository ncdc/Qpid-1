#ifndef QPID_MESSAGING_SYNCHIO_RECEIVER_GET_H
#define QPID_MESSAGING_SYNCHIO_RECEIVER_GET_H

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

#include "qpid/messaging/Duration.h"
#include "qpid/messaging/Message.h"
#include "qpid/messaging/Receiver.h"
#include "qpid/messaging/synchio/GetOrFetchImpl.h"

namespace qpid
{

  namespace messaging
  {

    namespace synchio
    {

      class ReceiverGet
      {

      public:
        ReceiverGet(qpid::messaging::Receiver& receiver,
                    qpid::messaging::Duration& timeout);
        ~ReceiverGet();

        void start();
        void stop();
        bool getSuccess();
        int getHandle();

        Message getMessage();

      private:
        GetOrFetchImpl* impl;

      };

    }

  }

}

#endif
