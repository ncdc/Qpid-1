#ifndef QPID_CLUSTER_EVENTHANDLER_H
#define QPID_CLUSTER_EVENTHANDLER_H

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

// TODO aconway 2010-10-19: experimental cluster code.

#include "types.h"
#include "Cpg.h"
#include "PollerDispatch.h"

namespace qpid {

namespace framing {
class AMQBody;
}

namespace cluster {
class Core;
class MessageHandler;

/**
 * Dispatch events received from CPG.
 * Thread unsafe: only called in CPG deliver thread context.
 */
class EventHandler : public Cpg::Handler
{
  public:
    EventHandler(Core&);
    ~EventHandler();

    void deliver( // CPG deliver callback.
        cpg_handle_t /*handle*/,
        const struct cpg_name *group,
        uint32_t /*nodeid*/,
        uint32_t /*pid*/,
        void* /*msg*/,
        int /*msg_len*/);

    void configChange( // CPG config change callback.
        cpg_handle_t /*handle*/,
        const struct cpg_name */*group*/,
        const struct cpg_address */*members*/, int /*nMembers*/,
        const struct cpg_address */*left*/, int /*nLeft*/,
        const struct cpg_address */*joined*/, int /*nJoined*/
    );


    MemberId getSender() { return sender; }
    MemberId getSelf() { return self; }
    Core& getCore() { return core; }
    Cpg& getCpg() { return cpg; }

  private:
    void invoke(const framing::AMQBody& body);

    Core& core;
    Cpg cpg;
    PollerDispatch dispatcher;
    MemberId sender;              // sender of current event.
    MemberId self;
    std::auto_ptr<MessageHandler> messageHandler;
};
}} // namespace qpid::cluster

#endif  /*!QPID_CLUSTER_EVENTHANDLER_H*/
