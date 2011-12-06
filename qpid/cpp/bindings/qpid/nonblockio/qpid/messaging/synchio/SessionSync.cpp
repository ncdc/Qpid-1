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

#include "qpid/messaging/synchio/SessionSync.h"
#include "qpid/messaging/synchio/SessionSyncImpl.h"

using namespace qpid::messaging;

namespace qpid
{

  namespace messaging
  {

    namespace synchio
    {

      SessionSync::SessionSync(Session& session)
        :impl(new SessionSyncImpl(session))
      { }
      SessionSync::~SessionSync()
      { delete impl; }

      void SessionSync::start() { impl->start(); }
      void SessionSync::stop()  { impl->stop(); }
      int  SessionSync::getHandle() { return impl->getHandle(); }
      bool SessionSync::getSuccess() { return impl->getSuccess(); }
    }

  }

}

