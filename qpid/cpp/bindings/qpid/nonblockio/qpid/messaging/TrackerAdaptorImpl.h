#ifndef QPID_CLIENT_AMQP0_10_TRACKER_ADAPTOR_IMPL_H
#define QPID_CLIENT_AMQP0_10_TRACKER_ADAPTOR_IMPL_H

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

#include "qpid/messaging/Message.h"
#include "qpid/messaging/Prong.h"
#include "qpid/messaging/Tracker.h"
#include "qpid/messaging/TrackerEventHandler.h"
#include "qpid/sys/Monitor.h"
#include "qpid/sys/Runnable.h"
#include "qpid/sys/Thread.h"

using namespace qpid::messaging;
using namespace qpid::sys;

namespace qpid
{

  namespace messaging
  {

    class TrackerAdaptorImpl : public Runnable
    {
    private:

      Tracker& tracker;
      bool incoming;
      bool outgoing;
      qpid::messaging::Duration timeout;
      TrackerEventHandler& eventHandler;
      Prong prong;

      bool threadCancelled;
      Thread* thread;
      Mutex lock;

    public:

      TrackerAdaptorImpl(Tracker& tracker,
                         bool incoming, bool outgoing,
                         qpid::messaging::Duration timeout,
                         TrackerEventHandler& eventHandler);
      virtual ~TrackerAdaptorImpl();

      int getHandle();

      void start();
      void stop();
      void run();
      bool isRunning();

    };

  }

}

#endif
