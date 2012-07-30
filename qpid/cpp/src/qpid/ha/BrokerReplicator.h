#ifndef QPID_HA_REPLICATOR_H
#define QPID_HA_REPLICATOR_H

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

#include "types.h"
#include "ReplicationTest.h"
#include "AlternateExchangeSetter.h"
#include "qpid/Address.h"
#include "qpid/broker/Exchange.h"
#include "qpid/types/Variant.h"
#include "qpid/management/ManagementObject.h"
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

namespace qpid {

namespace broker {
class Broker;
class Link;
class Bridge;
class SessionHandler;
}

namespace framing {
class FieldTable;
}

namespace ha {
class HaBroker;
class QueueReplicator;

/**
 * Replicate configuration on a backup broker.
 *
 * Implemented as an exchange that subscribes to receive QMF
 * configuration events from the primary. It configures local queues
 * exchanges and bindings to replicate the primary.
 * It also creates QueueReplicators for newly replicated queues.
 *
 * THREAD UNSAFE: Only called in Link connection thread, no need for locking.
 *
 */
class BrokerReplicator : public broker::Exchange,
                         public boost::enable_shared_from_this<BrokerReplicator>
{
  public:
    BrokerReplicator(HaBroker&, const boost::shared_ptr<broker::Link>&);
    ~BrokerReplicator();

    void initialize();

    // Exchange methods
    std::string getType() const;
    bool bind(boost::shared_ptr<broker::Queue>, const std::string&, const framing::FieldTable*);
    bool unbind(boost::shared_ptr<broker::Queue>, const std::string&, const framing::FieldTable*);
    void route(broker::Deliverable&);
    bool isBound(boost::shared_ptr<broker::Queue>, const std::string* const, const framing::FieldTable* const);

  private:
    typedef boost::shared_ptr<QueueReplicator> QueueReplicatorPtr;

    void initializeBridge(broker::Bridge&, broker::SessionHandler&);

    void doEventQueueDeclare(types::Variant::Map& values);
    void doEventQueueDelete(types::Variant::Map& values);
    void doEventExchangeDeclare(types::Variant::Map& values);
    void doEventExchangeDelete(types::Variant::Map& values);
    void doEventBind(types::Variant::Map&);
    void doEventUnbind(types::Variant::Map&);
    void doEventMembersUpdate(types::Variant::Map&);

    void doResponseQueue(types::Variant::Map& values);
    void doResponseExchange(types::Variant::Map& values);
    void doResponseBind(types::Variant::Map& values);
    void doResponseHaBroker(types::Variant::Map& values);

    QueueReplicatorPtr findQueueReplicator(const std::string& qname);
    void startQueueReplicator(const boost::shared_ptr<broker::Queue>&);
    void stopQueueReplicator(const std::string& name);

    boost::shared_ptr<broker::Queue> createQueue(
        const std::string& name,
        bool durable,
        bool autodelete,
        const qpid::framing::FieldTable& arguments,
        const std::string& alternateExchange);

    boost::shared_ptr<broker::Exchange> createExchange(
        const std::string& name,
        const std::string& type,
        bool durable,
        const qpid::framing::FieldTable& args,
        const std::string& alternateExchange);

    std::string logPrefix;
    std::string userId, remoteHost;
    ReplicationTest replicationTest;
    HaBroker& haBroker;
    broker::Broker& broker;
    boost::shared_ptr<broker::Link> link;
    bool initialized;
    AlternateExchangeSetter alternates;
    qpid::Address primary;
};
}} // namespace qpid::broker

#endif  /*!QPID_HA_REPLICATOR_H*/
