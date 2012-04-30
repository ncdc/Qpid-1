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

#include <unistd.h>
#include "qpid/messaging/synchio/Runner.h"

using namespace qpid::sys;

Runner* Runner::_instance = new Runner();

namespace qpid
{

  namespace messaging
  {

    namespace synchio
    {

      Runner::Runner():
        thread(NULL),
        cancelThread(false)
      {
        start();
      }


      Runner::~Runner()
      {
        if(thread)
          {
            stop();
          }
      }


      void Runner::start()
      {
        if(thread)
          {
            return;
          }

        Mutex::ScopedLock l(lock);

        thread = new Thread(*this);
        cancelThread = false;
      }


      void Runner::stop()
      {
        if(!thread)
          {
            return;
          }

        thread->join();
        delete thread;
        thread = NULL;
      }


      void Runner::run()
      {
        while(!cancelThread)
          {
            if(handlers.empty())
              {
                sleep(1);
              }
            else
              {
                SynchioCommand* next = NULL;

                {
                  Mutex::ScopedLock l(lock);

                  next = handlers.front();
                  handlers.pop();
                }

                if(next)
                  next->run();
              }
          }
      }


      void Runner::enqueue(SynchioCommand* handler)
      {
        Mutex::ScopedLock l(lock);

        // enqueue the handler
        handlers.push(handler);
      }


      Runner* Runner::instance()
      {
        return _instance;
      }


    }

  }

}
