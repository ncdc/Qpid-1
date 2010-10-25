#ifndef QPID_CLUSTER_CORE_H
#define QPID_CLUSTER_CORE_H

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

#include <string>
#include <memory>

#include "Cpg.h"
#include "MessageId.h"
#include "LockedMap.h"
#include <qpid/broker/QueuedMessage.h>

// TODO aconway 2010-10-19: experimental cluster code.

namespace qpid {

namespace framing{
class AMQBody;
}

namespace broker {
class Broker;
}

namespace cluster {
class EventHandler;
class BrokerHandler;

/**
 * Cluster core state machine.
 * Holds together the various objects that implement cluster behavior,
 * and holds state that is shared by multiple components.
 *
 * Thread safe: called from broker connection threads and CPG dispatch threads.
 */
class Core
{
  public:
    /** Configuration settings */
    struct Settings {
        std::string name;
    };

    typedef LockedMap<SequenceNumber, boost::intrusive_ptr<broker::Message> >
    SequenceMessageMap;

    /** Constructed during Plugin::earlyInitialize() */
    Core(const Settings&, broker::Broker&);

    /** Called during Plugin::initialize() */
    void initialize();

    /** Shut down broker due to fatal error. Caller should log a critical message */
    void fatal();

    /** Multicast an event */
    void mcast(const framing::AMQBody&);

    broker::Broker& getBroker() { return broker; }
    EventHandler& getEventHandler() { return *eventHandler; }
    BrokerHandler& getBrokerHandler() { return *brokerHandler; }

    /** Map of messages that are currently being routed.
     * Used to pass messages being routed from BrokerHandler to MessageHandler
     */
    SequenceMessageMap& getRoutingMap() { return routingMap; }
  private:
    broker::Broker& broker;
    std::auto_ptr<EventHandler> eventHandler; // Handles CPG events.
    BrokerHandler* brokerHandler; // Handles broker events.
    SequenceMessageMap routingMap;
};
}} // namespace qpid::cluster

#endif  /*!QPID_CLUSTER_CORE_H*/
