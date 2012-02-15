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

#include "qpid/messaging/synchio/NextReceiver.h"
#include "qpid/messaging/synchio/NextReceiverImpl.h"

using namespace qpid::messaging;

namespace qpid
{

  namespace messaging
  {

    namespace synchio
    {

      NextReceiver::NextReceiver(Session& session, Duration& timeout)
        :impl(new NextReceiverImpl(session, timeout))
      { }
      NextReceiver::~NextReceiver() { delete impl; }

      void NextReceiver::start() { impl->start(); }
      void NextReceiver::stop() { impl->stop(); }
      bool NextReceiver::isRunning() { return impl->isRunning(); }
      bool NextReceiver::getSuccess() { return impl->getSuccess(); }
      int NextReceiver::getHandle() { return impl->getHandle(); }
      Receiver NextReceiver::getReceiver() { return impl->getReceiver(); }

    }

  }

}