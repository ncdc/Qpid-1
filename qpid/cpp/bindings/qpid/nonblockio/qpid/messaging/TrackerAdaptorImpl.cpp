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

#include "TrackerAdaptorImpl.h"
#include "qpid/messaging/Duration.h"
#include "qpid/messaging/Receiver.h"
#include "qpid/messaging/exceptions.h"

namespace qpid
{

  namespace messaging
  {

    TrackerAdaptorImpl::TrackerAdaptorImpl(Tracker& tracker,
                                           bool incoming,
                                           bool outgoing,
                                           qpid::messaging::Duration timeout,
                                           TrackerEventHandler& eventHandler)
      :tracker(tracker)
      ,incoming(incoming)
      ,outgoing(outgoing)
      ,timeout(timeout)
      ,eventHandler(eventHandler)
      ,threadCancelled(false)
      ,thread(NULL)
    { }


    TrackerAdaptorImpl::~TrackerAdaptorImpl()
    { stop(); }


    int TrackerAdaptorImpl::getHandle()
    {
      return prong.getHandle();
    }

    void TrackerAdaptorImpl::start()
    {
      if(thread) return;

      threadCancelled = false;
      thread = new Thread(*this);
    }


    void TrackerAdaptorImpl::stop()
    {
      Mutex::ScopedLock l(lock);
      if(thread)
        {
          threadCancelled = true;
          thread->join();
          delete thread;
          thread = NULL;
        }
      prong.close();
    }


    void TrackerAdaptorImpl::run()
    {
      bool success = tracker.wait(timeout, incoming, outgoing);

      eventHandler.handleEvent(success);
      prong.updateHandles();
    }


    bool TrackerAdaptorImpl::isRunning()
    {
      return ((thread != NULL) && (!threadCancelled));
    }

  }

}


