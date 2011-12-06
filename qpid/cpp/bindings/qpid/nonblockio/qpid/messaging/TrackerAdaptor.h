#ifndef QPID_BINDING_TRACKER_ADAPTOR_H
#define QPID_BINDING_TRACKER_ADAPTOR_H

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

#include "qpid/messaging/ImportExport.h"
#include "qpid/messaging/Duration.h"
#include "qpid/messaging/Message.h"
#include "qpid/messaging/Tracker.h"
#include "qpid/messaging/TrackerEventHandler.h"

namespace qpid
{

  namespace messaging
  {

    class TrackerAdaptorImpl;

    class TrackerAdaptor
    {
    public:

      TrackerAdaptor(qpid::messaging::Tracker& tracker,
                                                bool incoming, bool outgoing,
                                                qpid::messaging::Duration timeout,
                                                qpid::messaging::TrackerEventHandler& handler);
      virtual ~TrackerAdaptor();

      int getHandle();

      void start();
      void stop();

      bool isRunning();

    private:

      TrackerAdaptorImpl* impl;

    };

  }

}

#endif
