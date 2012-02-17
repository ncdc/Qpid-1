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
#include "qpid/cluster/OutputInterceptor.h"
#include "qpid/cluster/Connection.h"
#include "qpid/cluster/Cluster.h"
#include "qpid/framing/ClusterConnectionDeliverDoOutputBody.h"
#include "qpid/framing/AMQFrame.h"
#include "qpid/log/Statement.h"
#include <boost/current_function.hpp>


namespace qpid {
namespace cluster {

using namespace framing;
using namespace std;

NoOpConnectionOutputHandler OutputInterceptor::discardHandler;

OutputInterceptor::OutputInterceptor(Connection& p, sys::ConnectionOutputHandler& h)
    : parent(p), closing(false), next(&h), sendMax(2048), sent(0), sentDoOutput(false)
{}

void OutputInterceptor::send(framing::AMQFrame& f) {
    sys::Mutex::ScopedLock l(lock);
    next->send(f);
}

void OutputInterceptor::activateOutput() {
    sys::Mutex::ScopedLock l(lock);
    if (parent.isCatchUp())
        next->activateOutput();
    else
        sendDoOutput(sendMax, l);
}

void OutputInterceptor::abort() {
    sys::Mutex::ScopedLock l(lock);
    if (parent.isLocal()) {
        next->abort();
    }
}

void OutputInterceptor::giveReadCredit(int32_t credit) {
    sys::Mutex::ScopedLock l(lock);
    next->giveReadCredit(credit);
}

// Called in write thread when the IO layer has no more data to write.
// We only process IO callbacks in the write thread during catch-up.
// Normally we run doOutput only on delivery of doOutput requests.
bool OutputInterceptor::doOutput() {
    parent.doCatchupIoCallbacks();
    return false;
}

// Send output up to limit, calculate new limit.
void OutputInterceptor::deliverDoOutput(uint32_t limit) {
    sys::Mutex::ScopedLock l(lock);
    sentDoOutput = false;
    sendMax = limit;
    size_t newLimit = limit;
    if (parent.isLocal()) {
        size_t buffered = next->getBuffered();
        if (buffered == 0 && sent == sendMax) // Could have sent more, increase the limit.
            newLimit = sendMax*2;
        else if (buffered > 0 && sent > 1) // Data left unsent, reduce the limit.
            newLimit = (sendMax + sent) / 2;
    }
    sent = 0;
    while (sent < limit) {
        {
            sys::Mutex::ScopedUnlock u(lock);
            if (!parent.getBrokerConnection()->doOutput()) break;
        }
        ++sent;
    }
    if (sent == limit) sendDoOutput(newLimit, l);
}

void OutputInterceptor::sendDoOutput(size_t newLimit, const sys::Mutex::ScopedLock&) {
    if (parent.isLocal() && !sentDoOutput && !closing && parent.isAnnounced()) {
        sentDoOutput = true;
        parent.getCluster().getMulticast().mcastControl(
            ClusterConnectionDeliverDoOutputBody(ProtocolVersion(), newLimit),
            parent.getId());
    }
}

// Called in connection thread when local connection closes.
void OutputInterceptor::closeOutput() {
    sys::Mutex::ScopedLock l(lock);
    closing = true;
    next = &discardHandler;
}

void OutputInterceptor::close() {
    sys::Mutex::ScopedLock l(lock);
    next->close();
}

size_t OutputInterceptor::getBuffered() const {
    sys::Mutex::ScopedLock l(lock);
    return next->getBuffered();
}

}} // namespace qpid::cluster
