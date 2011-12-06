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

#include "qpid/messaging/synchio/BaseTrackerEventHandler.h"

using namespace qpid::messaging;

namespace qpid
{

  namespace messaging
  {

    namespace synchio
    {

      BaseTrackerEventHandler::BaseTrackerEventHandler(Sender sender,
                                                       bool incoming, bool outgoing,
                                                       Duration timeout)
        :tracker()
        ,adaptor(TrackerAdaptor(tracker, incoming, outgoing, timeout, *this))
        ,success(false)
      { tracker.track(sender); }


      BaseTrackerEventHandler::BaseTrackerEventHandler(Session session,
                                                       bool incoming, bool outgoing,
                                                       Duration timeout)
        :tracker()
        ,adaptor(TrackerAdaptor(tracker, incoming, outgoing, timeout, *this))
        ,success(false)
      { tracker.track(session); }


      BaseTrackerEventHandler::~BaseTrackerEventHandler()
      { adaptor.stop(); }


      void BaseTrackerEventHandler::start()
      { adaptor.start(); }


      void BaseTrackerEventHandler::stop()
      { adaptor.stop(); }


      bool BaseTrackerEventHandler::isRunning()
      { return adaptor.isRunning(); }

      bool BaseTrackerEventHandler::getSuccess()
      { return success; }


      int BaseTrackerEventHandler::getHandle()
      { return adaptor.getHandle(); }


      void BaseTrackerEventHandler::handleEvent(bool success)
      { this->success = success; }

    }

  }

}

