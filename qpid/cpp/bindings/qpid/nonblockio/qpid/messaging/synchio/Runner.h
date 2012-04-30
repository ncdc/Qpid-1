#ifndef QPID_MESSAGING_SYNCHIO_RUNNER_H
#define QPID_MESSAGING_SYNCHIO_RUNNER_H

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

#include <queue>

#include "qpid/messaging/synchio/Runner.h"
#include "qpid/messaging/synchio/SynchioCommand.h"
#include "qpid/sys/Monitor.h"
#include "qpid/sys/Runnable.h"
#include "qpid/sys/Thread.h"

using namespace qpid::messaging::synchio;

namespace qpid
{

  namespace messaging
  {

    namespace synchio
    {

      class Runner : public qpid::sys::Runnable
        {

        public:

          Runner();
          ~Runner();

          void start();
          void stop();
          void run();

          void enqueue(SynchioCommand* command);

          int queueDepth() { return handlers.size(); }

          static Runner* instance();

        private:

          std::queue<SynchioCommand*>  handlers;
          qpid::sys::Mutex lock;
          qpid::sys::Thread* thread;
          bool cancelThread;
          static Runner* _instance;

        };

    }

  }

}

#endif
