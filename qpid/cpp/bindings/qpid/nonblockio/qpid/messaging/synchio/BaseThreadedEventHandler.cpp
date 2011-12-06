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

#include "qpid/messaging/synchio/BaseThreadedEventHandler.h"

using namespace qpid::sys;

namespace qpid
{

  namespace messaging
  {

    namespace synchio
    {

      BaseThreadedEventHandler::BaseThreadedEventHandler():
        thread(NULL),
        success(false)
      { }
      BaseThreadedEventHandler::~BaseThreadedEventHandler()
      {
        if(thread)
          {
            stop();
          }
      }


      void BaseThreadedEventHandler::start()
      {
        Mutex::ScopedLock l(lock);

        if(thread)
          {
            return;
          }

        thread = new Thread(*this);
      }


      void BaseThreadedEventHandler::stop()
      {
        Mutex::ScopedLock l(lock);

        if(!thread)
          {
            return;
          }

        thread->join();
        delete thread;
        thread = NULL;
        prong.close();
      }


      bool BaseThreadedEventHandler::isRunning()
      {
        return (thread != NULL);
      }


      int BaseThreadedEventHandler::getHandle()
      {
        return prong.getHandle();
      }

      bool BaseThreadedEventHandler::getSuccess()
      {
        return success;
      }


      void BaseThreadedEventHandler::notifyListeners(bool success)
      {
        this->success = success;
        prong.updateHandles();
      }

    }

  }

}
