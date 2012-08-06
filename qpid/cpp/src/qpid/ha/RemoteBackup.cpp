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
#include "RemoteBackup.h"
#include "QueueGuard.h"
#include "qpid/broker/Broker.h"
#include "qpid/broker/Queue.h"
#include "qpid/broker/QueueRegistry.h"
#include <boost/bind.hpp>

namespace qpid {
namespace ha {

using sys::Mutex;
using boost::bind;

RemoteBackup::RemoteBackup(const BrokerInfo& info, ReplicationTest rt, bool con) :
    logPrefix("Primary: Remote backup "+info.getLogId()+": "),
    brokerInfo(info), replicationTest(rt), connected(con), reportedReady(false)
{}

void RemoteBackup::setInitialQueues(broker::QueueRegistry& queues, bool createGuards)
{
    QPID_LOG(debug, logPrefix << "Setting initial queues" << (createGuards ? " and guards" : ""));
    queues.eachQueue(boost::bind(&RemoteBackup::initialQueue, this, _1, createGuards));
}

RemoteBackup::~RemoteBackup() { cancel(); }

void RemoteBackup::cancel() {
    for (GuardMap::iterator i = guards.begin(); i != guards.end(); ++i)
        i->second->cancel();
    guards.clear();
}

bool RemoteBackup::isReady() {
    return connected && initialQueues.empty();
}

void RemoteBackup::initialQueue(const QueuePtr& q, bool createGuard) {
    if (replicationTest.isReplicated(ALL, *q)) {
        initialQueues.insert(q);
        if (createGuard) guards[q].reset(new QueueGuard(*q, brokerInfo));
    }
}

RemoteBackup::GuardPtr RemoteBackup::guard(const QueuePtr& q) {
    GuardMap::iterator i = guards.find(q);
    GuardPtr guard;
    if (i != guards.end()) {
        guard = i->second;
        guards.erase(i);
    }
    return guard;
}

namespace {
typedef std::set<boost::shared_ptr<broker::Queue> > QS;
struct QueueSetPrinter {
    const QS& qs;
    std::string prefix;
    QueueSetPrinter(const std::string& p, const QS& q) : qs(q), prefix(p) {}
};
std::ostream& operator<<(std::ostream& o, const QueueSetPrinter& qp) {
    if (!qp.qs.empty()) o << qp.prefix;
    for (QS::const_iterator i = qp.qs.begin(); i != qp.qs.end(); ++i)
        o << (*i)->getName() << " ";
    return o;
}
}

void RemoteBackup::ready(const QueuePtr& q) {
    initialQueues.erase(q);
    QPID_LOG(debug, logPrefix << "Queue ready: " << q->getName()
             <<  QueueSetPrinter(", waiting for: ", initialQueues));
    if (isReady()) QPID_LOG(debug, logPrefix << "All queues ready");
}

// Called via ConfigurationObserver::queueCreate and from initialQueue
void RemoteBackup::queueCreate(const QueuePtr& q) {
    if (replicationTest.isReplicated(ALL, *q))
        guards[q].reset(new QueueGuard(*q, brokerInfo));
}

// Called via ConfigurationObserver
void RemoteBackup::queueDestroy(const QueuePtr& q) {
    initialQueues.erase(q);
    GuardMap::iterator i = guards.find(q);
    if (i != guards.end()) {
        i->second->cancel();
        guards.erase(i);
    }
}

bool RemoteBackup::reportReady() {
    if (!reportedReady && isReady()) {
        reportedReady = true;
        return true;
    }
    return false;
}

}} // namespace qpid::ha
