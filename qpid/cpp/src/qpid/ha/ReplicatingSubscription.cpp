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

#include "ReplicatingSubscription.h"
#include "qpid/broker/Queue.h"
#include "qpid/broker/SessionContext.h"
#include "qpid/broker/ConnectionState.h"
#include "qpid/framing/AMQFrame.h"
#include "qpid/framing/MessageTransferBody.h"
#include "qpid/log/Statement.h"
#include <sstream>

namespace qpid {
namespace ha {

using namespace framing;
using namespace broker;
using namespace std;

const string ReplicatingSubscription::QPID_REPLICATING_SUBSCRIPTION("qpid.replicating-subscription");

namespace {
const string DOLLAR("$");
const string INTERNAL("-internal");
} // namespace

string mask(const string& in)
{
    return DOLLAR + in + INTERNAL;
}

/* Called by SemanticState::consume to create a consumer */
boost::shared_ptr<broker::SemanticState::ConsumerImpl>
ReplicatingSubscription::Factory::create(
    SemanticState* parent,
    const string& name,
    Queue::shared_ptr queue,
    bool ack,
    bool acquire,
    bool exclusive,
    const string& tag,
    const string& resumeId,
    uint64_t resumeTtl,
    const framing::FieldTable& arguments
) {
    boost::shared_ptr<ReplicatingSubscription> rs;
    if (arguments.isSet(QPID_REPLICATING_SUBSCRIPTION)) {
        rs.reset(new ReplicatingSubscription(
                     parent, name, queue, ack, acquire, exclusive, tag,
                     resumeId, resumeTtl, arguments));
        queue->addObserver(rs);
    }
    return rs;
}

ReplicatingSubscription::ReplicatingSubscription(
    SemanticState* parent,
    const string& name,
    Queue::shared_ptr queue,
    bool ack,
    bool acquire,
    bool exclusive,
    const string& tag,
    const string& resumeId,
    uint64_t resumeTtl,
    const framing::FieldTable& arguments
) : ConsumerImpl(parent, name, queue, ack, acquire, exclusive, tag,
                 resumeId, resumeTtl, arguments),
    events(new Queue(mask(name))),
    consumer(new DelegatingConsumer(*this))
{
    stringstream ss;
    ss << "HA: Primary: " << getQueue()->getName() << " at "
       << parent->getSession().getConnection().getUrl() << ": ";
    logPrefix = ss.str();

    // FIXME aconway 2011-12-09: Failover optimization removed.
    // There was code here to re-use messages already on the backup
    // during fail-over. This optimization was removed to simplify
    // the logic till we get the basic replication stable, it
    // can be re-introduced later. Last revision with the optimization:
    // r1213258 | QPID-3603: Fix QueueReplicator subscription parameters.

    QPID_LOG(debug, logPrefix << "Created backup subscription " << getName());

    // FIXME aconway 2011-12-15: ConsumerImpl::position is left at 0
    // so we will start consuming from the lowest numbered message.
    // This is incorrect if the sequence number wraps around, but
    // this is what all consumers currently do.
}

// Message is delivered in the subscription's connection thread.
bool ReplicatingSubscription::deliver(QueuedMessage& m) {
    // Add position events for the subscribed queue, not for the internal event queue.
    if (m.queue && m.queue == getQueue().get()) {
        sys::Mutex::ScopedLock l(lock);
        assert(position == m.position);
        // m.position is the position of the newly enqueued m on the local queue.
        // backupPosition is latest position on the backup queue (before enqueueing m.)
        assert(m.position > backupPosition);
        if (m.position - backupPosition > 1) {
            // Position has advanced because of messages dequeued ahead of us.
            SequenceNumber send(m.position);
            --send;   // Send the position before m was enqueued.
            sendPositionEvent(send, l);
        }
        backupPosition = m.position;
        QPID_LOG(trace, logPrefix << "Replicating message " << m.position);
    }
    return ConsumerImpl::deliver(m);
}

ReplicatingSubscription::~ReplicatingSubscription() {}


// INVARIANT: delayed contains msg <=> we have outstanding startCompletion on msg

// Mark a message completed. May be called by acknowledge or dequeued
void ReplicatingSubscription::complete(
    const QueuedMessage& qm, const sys::Mutex::ScopedLock&)
{
    // Handle completions for the subscribed queue, not the internal event queue.
    if (qm.queue && qm.queue == getQueue().get()) {
        QPID_LOG(trace, logPrefix << "Completed message " << qm.position);
        Delayed::iterator i= delayed.find(qm.position);
        // The same message can be completed twice, by acknowledged and
        // dequeued, remove it from the set so it only gets completed
        // once.
        if (i != delayed.end()) {
            assert(i->second.payload == qm.payload);
            qm.payload->getIngressCompletion().finishCompleter();
            delayed.erase(i);
        }
    }
}

// Called before we get notified of the message being available and
// under the message lock in the queue. Called in arbitrary connection thread.
void ReplicatingSubscription::enqueued(const QueuedMessage& qm) {
    sys::Mutex::ScopedLock l(lock);
    // Delay completion
    QPID_LOG(trace, logPrefix << "Delaying completion of message " << qm.position);
    qm.payload->getIngressCompletion().startCompleter();
    assert(delayed.find(qm.position) == delayed.end());
    delayed[qm.position] = qm;
}


// Function to complete a delayed message, called by cancel()
void ReplicatingSubscription::cancelComplete(
    const Delayed::value_type& v, const sys::Mutex::ScopedLock&)
{
    QPID_LOG(trace, logPrefix << "Cancel completed message " << v.second.position);
    v.second.payload->getIngressCompletion().finishCompleter();
}

// Called in the subscription's connection thread.
void ReplicatingSubscription::cancel()
{
    getQueue()->removeObserver(
        boost::dynamic_pointer_cast<QueueObserver>(shared_from_this()));
    {
        sys::Mutex::ScopedLock l(lock);
        QPID_LOG(debug, logPrefix <<"Cancelled backup subscription " << getName());
        for_each(delayed.begin(), delayed.end(),
                 boost::bind(&ReplicatingSubscription::cancelComplete, this, _1, boost::ref(l)));
        delayed.clear();
    }
    ConsumerImpl::cancel();
}

// Called on primary in the backups IO thread.
void ReplicatingSubscription::acknowledged(const QueuedMessage& msg) {
    sys::Mutex::ScopedLock l(lock);
    // Finish completion of message, it has been acknowledged by the backup.
    complete(msg, l);
}

// Hide the "queue deleted" error for a ReplicatingSubscription when a
// queue is deleted, this is normal and not an error.
bool ReplicatingSubscription::hideDeletedError() { return true; }

// Called with lock held. Called in subscription's connection thread.
void ReplicatingSubscription::sendDequeueEvent(const sys::Mutex::ScopedLock& l)
{
    QPID_LOG(trace, logPrefix << "Sending dequeues " << dequeues);
    string buf(dequeues.encodedSize(),'\0');
    framing::Buffer buffer(&buf[0], buf.size());
    dequeues.encode(buffer);
    dequeues.clear();
    buffer.reset();
    sendEvent(QueueReplicator::DEQUEUE_EVENT_KEY, buffer, l);
}

// Called after the message has been removed from the deque and under
// the messageLock in the queue. Called in arbitrary connection threads.
void ReplicatingSubscription::dequeued(const QueuedMessage& qm)
{
    {
        sys::Mutex::ScopedLock l(lock);
        QPID_LOG(trace, logPrefix << "Dequeued message " << qm.position);
        dequeues.add(qm.position);
        // If we have not yet sent this message to the backup, then
        // complete it now as it will never be accepted.
        if (qm.position > position) complete(qm, l);
    }
    notify();                   // Ensure a call to doDispatch
}

// Called with lock held. Called in subscription's connection thread.
void ReplicatingSubscription::sendPositionEvent(
    SequenceNumber position, const sys::Mutex::ScopedLock&l )
{
    QPID_LOG(trace, logPrefix << "Sending position " << position
             << ", was " << backupPosition);
    string buf(backupPosition.encodedSize(),'\0');
    framing::Buffer buffer(&buf[0], buf.size());
    position.encode(buffer);
    buffer.reset();
    sendEvent(QueueReplicator::POSITION_EVENT_KEY, buffer, l);
}

void ReplicatingSubscription::sendEvent(const std::string& key, framing::Buffer& buffer,
                                        const sys::Mutex::ScopedLock&)
{
    //generate event message
    boost::intrusive_ptr<Message> event = new Message();
    AMQFrame method((MessageTransferBody(ProtocolVersion(), string(), 0, 0)));
    AMQFrame header((AMQHeaderBody()));
    AMQFrame content((AMQContentBody()));
    content.castBody<AMQContentBody>()->decode(buffer, buffer.getSize());
    header.setBof(false);
    header.setEof(false);
    header.setBos(true);
    header.setEos(true);
    content.setBof(false);
    content.setEof(true);
    content.setBos(true);
    content.setEos(true);
    event->getFrames().append(method);
    event->getFrames().append(header);
    event->getFrames().append(content);

    DeliveryProperties* props = event->getFrames().getHeaders()->get<DeliveryProperties>(true);
    props->setRoutingKey(key);
    // Send the event using the events queue. Consumer is a
    // DelegatingConsumer that delegates to *this for everything but
    // has an independnet position. We put an event on events and
    // dispatch it through ourselves to send it in line with the
    // normal browsing messages.
    events->deliver(event);
    events->dispatch(consumer);
}


// Called in subscription's connection thread.
bool ReplicatingSubscription::doDispatch()
{
    {
        sys::Mutex::ScopedLock l(lock);
        if (!dequeues.empty()) sendDequeueEvent(l);
    }
    return ConsumerImpl::doDispatch();
}

ReplicatingSubscription::DelegatingConsumer::DelegatingConsumer(ReplicatingSubscription& c) : Consumer(c.getName(), true), delegate(c) {}
ReplicatingSubscription::DelegatingConsumer::~DelegatingConsumer() {}
bool ReplicatingSubscription::DelegatingConsumer::deliver(QueuedMessage& m) { return delegate.deliver(m); }
void ReplicatingSubscription::DelegatingConsumer::notify() { delegate.notify(); }
bool ReplicatingSubscription::DelegatingConsumer::filter(boost::intrusive_ptr<Message> msg) { return delegate.filter(msg); }
bool ReplicatingSubscription::DelegatingConsumer::accept(boost::intrusive_ptr<Message> msg) { return delegate.accept(msg); }
bool ReplicatingSubscription::DelegatingConsumer::browseAcquired() const { return delegate.browseAcquired(); }
OwnershipToken* ReplicatingSubscription::DelegatingConsumer::getSession() { return delegate.getSession(); }

}} // namespace qpid::ha
