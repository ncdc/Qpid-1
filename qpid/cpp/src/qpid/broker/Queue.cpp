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

#include "qpid/broker/Broker.h"
#include "qpid/broker/Cluster.h"
#include "qpid/broker/Queue.h"
#include "qpid/broker/QueueEvents.h"
#include "qpid/broker/Exchange.h"
#include "qpid/broker/DeliverableMessage.h"
#include "qpid/broker/MessageStore.h"
#include "qpid/broker/NullMessageStore.h"
#include "qpid/broker/QueueRegistry.h"

#include "qpid/StringUtils.h"
#include "qpid/log/Statement.h"
#include "qpid/management/ManagementAgent.h"
#include "qpid/framing/reply_exceptions.h"
#include "qpid/framing/FieldTable.h"
#include "qpid/sys/ClusterSafe.h"
#include "qpid/sys/Monitor.h"
#include "qpid/sys/Time.h"
#include "qmf/org/apache/qpid/broker/ArgsQueuePurge.h"
#include "qmf/org/apache/qpid/broker/ArgsQueueReroute.h"

#include <iostream>
#include <algorithm>
#include <functional>

#include <boost/bind.hpp>
#include <boost/intrusive_ptr.hpp>


using namespace qpid::broker;
using namespace qpid::sys;
using namespace qpid::framing;
using qpid::management::ManagementAgent;
using qpid::management::ManagementObject;
using qpid::management::Manageable;
using qpid::management::Args;
using std::for_each;
using std::mem_fun;
namespace _qmf = qmf::org::apache::qpid::broker;


namespace 
{
const std::string qpidMaxSize("qpid.max_size");
const std::string qpidMaxCount("qpid.max_count");
const std::string qpidNoLocal("no-local");
const std::string qpidTraceIdentity("qpid.trace.id");
const std::string qpidTraceExclude("qpid.trace.exclude");
const std::string qpidLastValueQueue("qpid.last_value_queue");
const std::string qpidLastValueQueueNoBrowse("qpid.last_value_queue_no_browse");
const std::string qpidPersistLastNode("qpid.persist_last_node");
const std::string qpidVQMatchProperty("qpid.LVQ_key");
const std::string qpidQueueEventGeneration("qpid.queue_event_generation");
//following feature is not ready for general use as it doesn't handle
//the case where a message is enqueued on more than one queue well enough:
const std::string qpidInsertSequenceNumbers("qpid.insert_sequence_numbers");

const int ENQUEUE_ONLY=1;
const int ENQUEUE_AND_DEQUEUE=2;
}

Queue::Queue(const string& _name, bool _autodelete, 
             MessageStore* const _store,
             const OwnershipToken* const _owner,
             Manageable* parent,
             Broker* b) :

    name(_name), 
    autodelete(_autodelete),
    store(_store),
    owner(_owner), 
    consumerCount(0),
    exclusive(0),
    noLocal(false),
    lastValueQueue(false),
    lastValueQueueNoBrowse(false),
    persistLastNode(false),
    inLastNodeFailure(false),
    persistenceId(0),
    policyExceeded(false),
    mgmtObject(0),
    eventMode(0),
    eventMgr(0),
    insertSeqNo(0),
    broker(b),
    deleted(false),
    barrier(*this)
{
    if (parent != 0 && broker != 0) {
        ManagementAgent* agent = broker->getManagementAgent();

        if (agent != 0) {
            mgmtObject = new _qmf::Queue(agent, this, parent, _name, _store != 0, _autodelete, _owner != 0);
            agent->addObject(mgmtObject, 0, store != 0);
        }
    }
}

Queue::~Queue()
{
    if (mgmtObject != 0)
        mgmtObject->resourceDestroy();
}

bool isLocalTo(const OwnershipToken* token, boost::intrusive_ptr<Message>& msg)
{
    return token && token->isLocal(msg->getPublisher());
}

bool Queue::isLocal(boost::intrusive_ptr<Message>& msg)
{
    //message is considered local if it was published on the same
    //connection as that of the session which declared this queue
    //exclusive (owner) or which has an exclusive subscription
    //(exclusive)
    return noLocal && (isLocalTo(owner, msg) || isLocalTo(exclusive, msg));
}

bool Queue::isExcluded(boost::intrusive_ptr<Message>& msg)
{
    return traceExclude.size() && msg->isExcluded(traceExclude);
}

void Queue::deliver(boost::intrusive_ptr<Message> msg){
    // Check for deferred delivery in a cluster.
    if (broker && broker->deferDelivery(name, msg))
        return;
    if (msg->isImmediate() && getConsumerCount() == 0) {
        if (alternateExchange) {
            DeliverableMessage deliverable(msg);
            alternateExchange->route(deliverable, msg->getRoutingKey(), msg->getApplicationHeaders());
        }
    } else if (isLocal(msg)) {
        //drop message
        QPID_LOG(info, "Dropping 'local' message from " << getName());
    } else if (isExcluded(msg)) {
        //drop message
        QPID_LOG(info, "Dropping excluded message from " << getName());
    } else {
        // if no store then mark as enqueued
        if (!enqueue(0, msg)){
            push(msg);
            msg->enqueueComplete();
        }else {
            push(msg);
        }
        mgntEnqStats(msg);
        QPID_LOG(debug, "Message " << msg << " enqueued on " << name);
    }
}

void Queue::recoverPrepared(boost::intrusive_ptr<Message>& msg)
{
    if (policy.get()) policy->recoverEnqueued(msg);
}

void Queue::recover(boost::intrusive_ptr<Message>& msg){
    if (policy.get()) policy->recoverEnqueued(msg);

    push(msg, true);
    if (store){ 
        // setup synclist for recovered messages, so they don't get re-stored on lastNodeFailure
        msg->addToSyncList(shared_from_this(), store); 
    }
    msg->enqueueComplete(); // mark the message as enqueued
    mgntEnqStats(msg);

    if (store && (!msg->isContentLoaded() || msg->checkContentReleasable())) {
        //content has not been loaded, need to ensure that lazy loading mode is set:
        //TODO: find a nicer way to do this
        msg->releaseContent(store);
        // NOTE: The log message in this section are used for flow-to-disk testing (which checks the log for the
        // presence of this message). Do not change this without also checking these tests.
        QPID_LOG(debug, "Message id=\"" << msg->getProperties<MessageProperties>()->getMessageId() << "\"; pid=0x" <<
                        std::hex << msg->getPersistenceId() << std::dec << ": Content released after recovery");
    }
}

void Queue::process(boost::intrusive_ptr<Message>& msg){
    push(msg);
    mgntEnqStats(msg);
    if (mgmtObject != 0){
        mgmtObject->inc_msgTxnEnqueues ();
        mgmtObject->inc_byteTxnEnqueues (msg->contentSize ());
    }
}

void Queue::requeue(const QueuedMessage& msg){
    assertClusterSafe();
    QueueListeners::NotificationSet copy;
    {    
        Mutex::ScopedLock locker(messageLock);
        if (!isEnqueued(msg)) return;
        msg.payload->enqueueComplete(); // mark the message as enqueued
        messages.insert(lower_bound(messages.begin(), messages.end(), msg), msg);
        listeners.populate(copy);

        // for persistLastNode - don't force a message twice to disk, but force it if no force before 
        if(inLastNodeFailure && persistLastNode && !msg.payload->isStoredOnQueue(shared_from_this())) {
            msg.payload->forcePersistent();
            if (msg.payload->isForcedPersistent() ){
                boost::intrusive_ptr<Message> payload = msg.payload;
            	enqueue(0, payload);
            }
        }
    }
    if (broker) broker->getCluster().release(msg);
    copy.notify();
}

void Queue::clearLVQIndex(const QueuedMessage& msg){
    assertClusterSafe();
    const framing::FieldTable* ft = msg.payload ? msg.payload->getApplicationHeaders() : 0;
    if (lastValueQueue && ft){
        string key = ft->getAsString(qpidVQMatchProperty);
        lvq.erase(key);
    }
}

// Inform the cluster of an acquired message on exit from a function
// that does the acquiring. The calling function should set qmsg
// to the acquired message.
struct ClusterAcquireOnExit {
    Broker* broker;
    QueuedMessage qmsg;
    ClusterAcquireOnExit(Broker* b) : broker(b) {}
    ~ClusterAcquireOnExit() {
        if (broker && qmsg.queue) broker->getCluster().acquire(qmsg);
    }
};

bool Queue::acquireMessageAt(const SequenceNumber& position, QueuedMessage& message) 
{
    ClusterAcquireOnExit willAcquire(broker);

    Mutex::ScopedLock locker(messageLock);
    assertClusterSafe();
    QPID_LOG(debug, "Attempting to acquire message at " << position);
    
    Messages::iterator i = findAt(position); 
    if (i != messages.end() ) {
        message = *i;
        if (lastValueQueue) {
            clearLVQIndex(*i);
        }
        QPID_LOG(debug, "Acquired message at " << i->position << " from " << name);
        willAcquire.qmsg = *i;
        messages.erase(i);
        return true;
    }
    QPID_LOG(debug, "Could not acquire message at " << position << " from " << name << "; no message at that position");
    return false;
}

bool Queue::acquire(const QueuedMessage& msg) {
    ClusterAcquireOnExit acquire(broker);

    Mutex::ScopedLock locker(messageLock);
    assertClusterSafe();

    QPID_LOG(debug, "attempting to acquire " << msg.position);
    Messages::iterator i = findAt(msg.position); 
    if ((i != messages.end() && i->position == msg.position) && // note that in some cases payload not be set
        (!lastValueQueue ||
         (lastValueQueue && msg.payload.get() == checkLvqReplace(*i).payload.get()) ) // note this is safe for no payload set 0==0
    )  {

        clearLVQIndex(msg);
        QPID_LOG(debug,
                 "Match found, acquire succeeded: " <<
                 i->position << " == " << msg.position);
        acquire.qmsg = *i;
        messages.erase(i);
        return true;
    }
    
    QPID_LOG(debug, "Acquire failed for " << msg.position);
    return false;
}

void Queue::notifyListener()
{
    assertClusterSafe();
    QueueListeners::NotificationSet set;
    {
        Mutex::ScopedLock locker(messageLock);
        if (messages.size()) {
            listeners.populate(set);
        }
    }
    set.notify();
}

bool Queue::getNextMessage(QueuedMessage& m, Consumer::shared_ptr c)
{
    checkNotDeleted();
    if (c->preAcquires()) {
        switch (consumeNextMessage(m, c)) {
          case CONSUMED:
            return true;
          case CANT_CONSUME:
            notifyListener();//let someone else try
          case NO_MESSAGES:
          default:
            return false;
        }        
    } else {
        return browseNextMessage(m, c);
    }
}

Queue::ConsumeCode Queue::consumeNextMessage(QueuedMessage& m, Consumer::shared_ptr c)
{
    while (true) {
        ClusterAcquireOnExit willAcquire(broker); // Outside the lock

        Mutex::ScopedLock locker(messageLock);
        if (messages.empty()) { 
            QPID_LOG(debug, "No messages to dispatch on queue '" << name << "'");
            listeners.addListener(c);
            return NO_MESSAGES;
        } else {
            QueuedMessage msg = getFront();
            if (msg.payload->hasExpired()) {
                QPID_LOG(debug, "Message expired from queue '" << name << "'");
                popAndDequeue();
                continue;
            }

            if (c->filter(msg.payload)) {
                if (c->accept(msg.payload)) {            
                    m = msg;
                    willAcquire.qmsg = msg;
                    popMsg(msg);
                    return CONSUMED;
                } else {
                    //message(s) are available but consumer hasn't got enough credit
                    QPID_LOG(debug, "Consumer can't currently accept message from '" << name << "'");
                    return CANT_CONSUME;
                }
            } else {
                //consumer will never want this message
                QPID_LOG(debug, "Consumer doesn't want message from '" << name << "'");
                return CANT_CONSUME;
            } 
        }
    }
}


bool Queue::browseNextMessage(QueuedMessage& m, Consumer::shared_ptr c)
{
    QueuedMessage msg(this);
    while (seek(msg, c)) {
        if (c->filter(msg.payload) && !msg.payload->hasExpired()) {
            if (c->accept(msg.payload)) {
                //consumer wants the message
                c->position = msg.position;
                m = msg;
                if (!lastValueQueueNoBrowse) clearLVQIndex(msg);
                if (lastValueQueue) {
                    boost::intrusive_ptr<Message> replacement = msg.payload->getReplacementMessage(this);
                    if (replacement.get()) m.payload = replacement;
                }
                return true;
            } else {
                //browser hasn't got enough credit for the message
                QPID_LOG(debug, "Browser can't currently accept message from '" << name << "'");
                return false;
            }
        } else {
            //consumer will never want this message, continue seeking
            c->position = msg.position;
            QPID_LOG(debug, "Browser skipping message from '" << name << "'");
        }
    }
    return false;
}

void Queue::removeListener(Consumer::shared_ptr c)
{
    QueueListeners::NotificationSet set;
    {
        Mutex::ScopedLock locker(messageLock);
        listeners.removeListener(c);
        if (messages.size()) {
            listeners.populate(set);
        }
    }
    set.notify();
}

bool Queue::dispatch(Consumer::shared_ptr c)
{
    QueuedMessage msg(this);
    if (getNextMessage(msg, c)) {
        c->deliver(msg);
        return true;
    } else {
        return false;
    }
}

// Find the next message 
bool Queue::seek(QueuedMessage& msg, Consumer::shared_ptr c) {
    Mutex::ScopedLock locker(messageLock);
    if (!messages.empty() && messages.back().position > c->position) {
        if (c->position < getFront().position) {
            msg = getFront();
            return true;
        } else {        
            Messages::iterator pos = findAt(c->position);
            if (pos != messages.end() && pos+1 != messages.end()) {
                msg = *(pos+1);
                return true;
            }
        }
    }
    listeners.addListener(c);
    return false;
}

Queue::Messages::iterator Queue::findAt(SequenceNumber pos) {

    if(!messages.empty()){
        QueuedMessage compM;
        compM.position = pos;
        unsigned long diff = pos.getValue() - messages.front().position.getValue();
        long maxEnd = diff < messages.size()? diff : messages.size();

        Messages::iterator i = lower_bound(messages.begin(),messages.begin()+maxEnd,compM); 
        if (i!= messages.end() && i->position == pos)
            return i;
    }    
    return messages.end(); // no match found.
}


QueuedMessage Queue::find(SequenceNumber pos) const {

    Mutex::ScopedLock locker(messageLock);
    if(!messages.empty()){
        QueuedMessage compM;
        compM.position = pos;
        unsigned long diff = pos.getValue() - messages.front().position.getValue();
        long maxEnd = diff < messages.size()? diff : messages.size();

        Messages::const_iterator i = lower_bound(messages.begin(),messages.begin()+maxEnd,compM); 
        if (i != messages.end())
            return *i;
    }
    return QueuedMessage();
}

void Queue::consume(Consumer::shared_ptr c, bool requestExclusive) {
    assertClusterSafe();
    size_t consumers;
    {
        Mutex::ScopedLock locker(consumerLock);
        if(exclusive) {
            throw ResourceLockedException(
                QPID_MSG("Queue " << getName() << " has an exclusive consumer. No more consumers allowed."));
        } else if(requestExclusive) {
            if(consumerCount) {
                throw ResourceLockedException(
                    QPID_MSG("Queue " << getName() << " already has consumers. Exclusive access denied."));
            } else {
                exclusive = c->getSession();
            }
        }
        consumers = ++consumerCount;
        if (mgmtObject != 0)
            mgmtObject->inc_consumerCount ();
    }
    if (broker) broker->getCluster().consume(*this, consumers);
}

void Queue::cancel(Consumer::shared_ptr c){
    removeListener(c);
    size_t consumers;
    {
        Mutex::ScopedLock locker(consumerLock);
        consumers = --consumerCount;
        if(exclusive) exclusive = 0;
        if (mgmtObject != 0)
            mgmtObject->dec_consumerCount ();
    }
    if (broker) broker->getCluster().cancel(*this, consumers);
}

QueuedMessage Queue::get(){
    ClusterAcquireOnExit acquire(broker); // Outside lock

    Mutex::ScopedLock locker(messageLock);
    QueuedMessage msg(this);

    if(!messages.empty()){
        msg = getFront();
        acquire.qmsg = msg;
        popMsg(msg);
    }
    return msg;
}

void Queue::purgeExpired()
{
    //As expired messages are discarded during dequeue also, only
    //bother explicitly expiring if the rate of dequeues since last
    //attempt is less than one per second.  

    //Note: This method is currently called periodically on the timer
    //thread. In a clustered broker this means that the purging does
    //not occur on the cluster event dispatch thread and consequently
    //that is not totally ordered w.r.t other events (including
    //publication of messages). However the cluster does ensure that
    //the actual expiration of messages (as distinct from the removing
    //of those expired messages from the queue) *is* consistently
    //ordered w.r.t. cluster events. This means that delivery of
    //messages is in general consistent across the cluster inspite of
    //any non-determinism in the triggering of a purge. However at
    //present purging a last value queue could potentially cause
    //inconsistencies in the cluster (as the order w.r.t publications
    //can affect the order in which messages appear in the
    //queue). Consequently periodic purging of an LVQ is not enabled
    //(expired messages will be removed on delivery and consolidated
    //by key as part of normal LVQ operation).

    if (dequeueTracker.sampleRatePerSecond() < 1 && !lastValueQueue) {
        Messages expired;
        {
            Mutex::ScopedLock locker(messageLock);
            for (Messages::iterator i = messages.begin(); i != messages.end();) {
                //Re-introduce management of LVQ-specific state here
                //if purging is renabled for that case (see note above)
                if (i->payload->hasExpired()) {
                    expired.push_back(*i);
                    i = messages.erase(i);
                } else {
                    ++i;
                }
            }
        }
        for_each(expired.begin(), expired.end(), bind(&Queue::dequeue, this, (TransactionContext*) 0, _1));
    }
}

/**
 * purge - for purging all or some messages on a queue
 *         depending on the purge_request
 *
 * purge_request == 0 then purge all messages
 *               == N then purge N messages from queue
 * Sometimes purge_request == 1 to unblock the top of queue
 *
 * The dest exchange may be supplied to re-route messages through the exchange.
 * It is safe to re-route messages such that they arrive back on the same queue,
 * even if the queue is ordered by priority.
 */
uint32_t Queue::purge(const uint32_t purge_request, boost::shared_ptr<Exchange> dest)
{
    Mutex::ScopedLock locker(messageLock);
    uint32_t purge_count = purge_request; // only comes into play if  >0 
    std::deque<DeliverableMessage> rerouteQueue;

    uint32_t count = 0;
    // Either purge them all or just the some (purge_count) while the queue isn't empty.
    while((!purge_request || purge_count--) && !messages.empty()) {
        if (dest.get()) {
            //
            // If there is a destination exchange, stage the messages onto a reroute queue
            // so they don't wind up getting purged more than once.
            //
            DeliverableMessage msg(getFront().payload);
            rerouteQueue.push_back(msg);
        }
        popAndDequeue();
        count++;
    }

    //
    // Re-route purged messages into the destination exchange.  Note that there's no need
    // to test dest.get() here because if it is NULL, the rerouteQueue will be empty.
    //
    while (!rerouteQueue.empty()) {
        DeliverableMessage msg(rerouteQueue.front());
        rerouteQueue.pop_front();
        dest->route(msg, msg.getMessage().getRoutingKey(),
                    msg.getMessage().getApplicationHeaders());
    }

    return count;
}

uint32_t Queue::move(const Queue::shared_ptr destq, uint32_t qty) {
    Mutex::ScopedLock locker(messageLock);
    uint32_t move_count = qty; // only comes into play if  qty >0 
    uint32_t count = 0; // count how many were moved for returning

    while((!qty || move_count--) && !messages.empty()) {
        QueuedMessage qmsg = getFront();
        boost::intrusive_ptr<Message> msg = qmsg.payload;
        destq->deliver(msg); // deliver message to the destination queue
        popMsg(qmsg);
        dequeue(0, qmsg);
        count++;
    }
    return count;
}

void Queue::popMsg(QueuedMessage& qmsg)
{
    assertClusterSafe();
    const framing::FieldTable* ft = qmsg.payload->getApplicationHeaders();
    if (lastValueQueue && ft){
        string key = ft->getAsString(qpidVQMatchProperty);
        lvq.erase(key);
    }
    messages.pop_front();
    ++dequeueTracker;
}

void Queue::push(boost::intrusive_ptr<Message>& msg, bool isRecovery){
    assertClusterSafe();
    QueuedMessage qm;
    QueueListeners::NotificationSet copy;
    {
        Mutex::ScopedLock locker(messageLock);   
        qm = QueuedMessage(this, msg, ++sequence);
        if (insertSeqNo) msg->getOrInsertHeaders().setInt64(seqNoKey, sequence);
         
        LVQ::iterator i;
        const framing::FieldTable* ft = msg->getApplicationHeaders();
        if (lastValueQueue && ft){
            string key = ft->getAsString(qpidVQMatchProperty);

            i = lvq.find(key);
            if (i == lvq.end() || (broker && broker->isClusterUpdatee())) {
                messages.push_back(qm);
                listeners.populate(copy);
                lvq[key] = msg; 
            }else {
                boost::intrusive_ptr<Message> old = i->second->getReplacementMessage(this);
                if (!old) old = i->second;
                i->second->setReplacementMessage(msg,this);
                // FIXME aconway 2010-10-15: it is incorrect to use qm.position below
                // should be using the position of the message being replaced.
                if (isRecovery) {
                    //can't issue new requests for the store until
                    //recovery is complete
                    pendingDequeues.push_back(QueuedMessage(qm.queue, old, qm.position));
                } else {
                    Mutex::ScopedUnlock u(messageLock);
                    dequeue(0, QueuedMessage(qm.queue, old, qm.position));
                }
            }           
        }else {
            messages.push_back(qm);
            listeners.populate(copy);
        }
        if (eventMode) {
            if (eventMgr) eventMgr->enqueued(qm);
            else QPID_LOG(warning, "Enqueue manager not set, events not generated for " << getName());
        }
        if (policy.get()) {
            policy->enqueued(qm);
        }
    }
    copy.notify();
    if (broker) broker->getCluster().enqueue(qm);
}

QueuedMessage Queue::getFront()
{
    QueuedMessage msg = messages.front();
    if (lastValueQueue) {
        boost::intrusive_ptr<Message> replacement = msg.payload->getReplacementMessage(this);
        if (replacement.get()) msg.payload = replacement;
    }
    return msg;
}

QueuedMessage& Queue::checkLvqReplace(QueuedMessage& msg)
{
    boost::intrusive_ptr<Message> replacement = msg.payload->getReplacementMessage(this);
    if (replacement.get()) {
        const framing::FieldTable* ft = replacement->getApplicationHeaders();
        if (ft) {
            string key = ft->getAsString(qpidVQMatchProperty);
            if (lvq.find(key) != lvq.end()){
                lvq[key] = replacement; 
            }        
        }
        msg.payload = replacement;
    }
    return msg;
}

/** function only provided for unit tests, or code not in critical message path */
uint32_t Queue::getEnqueueCompleteMessageCount() const
{
    Mutex::ScopedLock locker(messageLock);
    uint32_t count = 0;
    for ( Messages::const_iterator i = messages.begin(); i != messages.end(); ++i ) {
        //NOTE: don't need to use checkLvqReplace() here as it
        //is only relevant for LVQ which does not support persistence
        //so the enqueueComplete check has no effect
        if ( i->payload->isEnqueueComplete() ) count ++;
    }
    
    return count;
}

uint32_t Queue::getMessageCount() const
{
    Mutex::ScopedLock locker(messageLock);
    return messages.size();
}

uint32_t Queue::getConsumerCount() const
{
    Mutex::ScopedLock locker(consumerLock);
    return consumerCount;
}

bool Queue::canAutoDelete() const
{
    Mutex::ScopedLock locker(consumerLock);
    return autodelete && !consumerCount;
}

void Queue::clearLastNodeFailure()
{
    inLastNodeFailure = false;
}

void Queue::setLastNodeFailure()
{
    if (persistLastNode){
        Mutex::ScopedLock locker(messageLock);
        try {
    	    for ( Messages::iterator i = messages.begin(); i != messages.end(); ++i ) {
                if (lastValueQueue) checkLvqReplace(*i);
                // don't force a message twice to disk.
                if(!i->payload->isStoredOnQueue(shared_from_this())) {
                    i->payload->forcePersistent();
                    if (i->payload->isForcedPersistent() ){
            	        enqueue(0, i->payload);
                    }
                }
    	    }
        } catch (const std::exception& e) {
            // Could not go into last node standing (for example journal not large enough)
            QPID_LOG(error, "Unable to fail to last node standing for queue: " << name << " : " << e.what());
        }
        inLastNodeFailure = true;
    }
}


// return true if store exists, 
bool Queue::enqueue(TransactionContext* ctxt, boost::intrusive_ptr<Message>& msg, bool suppressPolicyCheck)
{
    ScopedUse u(barrier);
    if (!u.acquired) return false;

    if (policy.get() && !suppressPolicyCheck) {
        Messages dequeues;
        {
            Mutex::ScopedLock locker(messageLock);
            policy->tryEnqueue(msg);
            policy->getPendingDequeues(dequeues);
        }
        //depending on policy, may have some dequeues that need to performed without holding the lock
        for_each(dequeues.begin(), dequeues.end(), boost::bind(&Queue::dequeue, this, (TransactionContext*) 0, _1));        
    }

    if (inLastNodeFailure && persistLastNode){
        msg->forcePersistent();
    }
       
    if (traceId.size()) {
        //copy on write: take deep copy of message before modifying it
        //as the frames may already be available for delivery on other
        //threads
        boost::intrusive_ptr<Message> copy(new Message(*msg));
        msg = copy;
        msg->addTraceId(traceId);
    }

    if ((msg->isPersistent() || msg->checkContentReleasable()) && store) {
        msg->enqueueAsync(shared_from_this(), store); //increment to async counter -- for message sent to more than one queue
        boost::intrusive_ptr<PersistableMessage> pmsg = boost::static_pointer_cast<PersistableMessage>(msg);
        store->enqueue(ctxt, pmsg, *this);
        return true;
    }
    if (!store) {
        //Messages enqueued on a transient queue should be prevented
        //from having their content released as it may not be
        //recoverable by these queue for delivery
        msg->blockContentRelease();
    }
    return false;
}

void Queue::enqueueAborted(boost::intrusive_ptr<Message> msg)
{
    Mutex::ScopedLock locker(messageLock);
    if (policy.get()) policy->enqueueAborted(msg);       
}

void Queue::accept(TransactionContext* ctxt, const QueuedMessage& msg) {
    if (broker) broker->getCluster().accept(msg);
    dequeue(ctxt, msg);
}

struct ScopedClusterReject {
    Broker* broker;
    const QueuedMessage& qmsg;
    ScopedClusterReject(Broker* b, const QueuedMessage& m) : broker(b), qmsg(m) {
        if (broker) broker->getCluster().reject(qmsg);
    }
    ~ScopedClusterReject() {
        if (broker) broker->getCluster().rejected(qmsg);
    }
};

void Queue::reject(const QueuedMessage &msg) {
    ScopedClusterReject scr(broker, msg);
    Exchange::shared_ptr alternate = getAlternateExchange();
    if (alternate) {
        DeliverableMessage delivery(msg.payload);
        alternate->route(delivery, msg.payload->getRoutingKey(), msg.payload->getApplicationHeaders());
        QPID_LOG(info, "Routed rejected message from " << getName() << " to " 
                 << alternate->getName());
    } else {
        //just drop it
        QPID_LOG(info, "Dropping rejected message from " << getName());
    }
    dequeue(0, msg);
}

// return true if store exists, 
bool Queue::dequeue(TransactionContext* ctxt, const QueuedMessage& msg)
{
    ScopedUse u(barrier);
    if (!u.acquired) return false;
    {
        Mutex::ScopedLock locker(messageLock);
        if (!isEnqueued(msg)) return false;
        if (!ctxt) { 
            dequeued(msg);
        }
    }
    // This check prevents messages which have been forced persistent on one queue from dequeuing
    // from another on which no forcing has taken place and thus causing a store error.
    bool fp = msg.payload->isForcedPersistent();
    if (!fp || (fp && msg.payload->isStoredOnQueue(shared_from_this()))) {
        if ((msg.payload->isPersistent() || msg.payload->checkContentReleasable()) && store) {
            msg.payload->dequeueAsync(shared_from_this(), store); //increment to async counter -- for message sent to more than one queue
            boost::intrusive_ptr<PersistableMessage> pmsg = boost::static_pointer_cast<PersistableMessage>(msg.payload);
            store->dequeue(ctxt, pmsg, *this);
            return true;
        }
    }
    return false;
}

void Queue::dequeueCommitted(const QueuedMessage& msg)
{
    Mutex::ScopedLock locker(messageLock);
    dequeued(msg);    
    if (mgmtObject != 0) {
        mgmtObject->inc_msgTxnDequeues();
        mgmtObject->inc_byteTxnDequeues(msg.payload->contentSize());
    }
}

/**
 * Removes a message from the in-memory delivery queue as well
 * dequeing it from the logical (and persistent if applicable) queue
 */
void Queue::popAndDequeue()
{
    QueuedMessage msg = getFront();
    popMsg(msg);
    dequeue(0, msg);
}

/**
 * Updates policy and management when a message has been dequeued,
 * expects messageLock to be held
 */
void Queue::dequeued(const QueuedMessage& msg)
{
    // Note: Cluster::dequeued does only local book-keeping, no multicast
    // So OK to call here with lock held.
    if (broker) broker->getCluster().dequeue(msg);
    if (policy.get()) policy->dequeued(msg);
    mgntDeqStats(msg.payload);
    if (eventMode == ENQUEUE_AND_DEQUEUE && eventMgr) {
        eventMgr->dequeued(msg);
    }
}


void Queue::create(const FieldTable& _settings)
{
    settings = _settings;
    if (store) {
        store->create(*this, _settings);
    }
    configure(_settings);
    if (broker) broker->getCluster().create(*this);
}

void Queue::configure(const FieldTable& _settings, bool recovering)
{

    eventMode = _settings.getAsInt(qpidQueueEventGeneration);

    if (QueuePolicy::getType(_settings) == QueuePolicy::FLOW_TO_DISK && 
        (!store || NullMessageStore::isNullStore(store) || (eventMode && eventMgr && !eventMgr->isSync()) )) {
        if ( NullMessageStore::isNullStore(store)) {
            QPID_LOG(warning, "Flow to disk not valid for non-persisted queue:" << getName());
        } else if (eventMgr && !eventMgr->isSync() ) {
            QPID_LOG(warning, "Flow to disk not valid with async Queue Events:" << getName());
        }
        FieldTable copy(_settings);
        copy.erase(QueuePolicy::typeKey);
        setPolicy(QueuePolicy::createQueuePolicy(getName(), copy));
    } else {
        setPolicy(QueuePolicy::createQueuePolicy(getName(), _settings));
    }
    //set this regardless of owner to allow use of no-local with exclusive consumers also
    noLocal = _settings.get(qpidNoLocal);
    QPID_LOG(debug, "Configured queue " << getName() << " with no-local=" << noLocal);

    lastValueQueue= _settings.get(qpidLastValueQueue);
    if (lastValueQueue) QPID_LOG(debug, "Configured queue as Last Value Queue for: " << getName());

    lastValueQueueNoBrowse = _settings.get(qpidLastValueQueueNoBrowse);
    if (lastValueQueueNoBrowse){
        QPID_LOG(debug, "Configured queue as Last Value Queue No Browse for: " << getName());
        lastValueQueue = lastValueQueueNoBrowse;
    }
    
    persistLastNode= _settings.get(qpidPersistLastNode);
    if (persistLastNode) QPID_LOG(debug, "Configured queue to Persist data if cluster fails to one node for: " << getName());

    traceId = _settings.getAsString(qpidTraceIdentity);
    std::string excludeList = _settings.getAsString(qpidTraceExclude);
    if (excludeList.size()) {
        split(traceExclude, excludeList, ", ");
    }
    QPID_LOG(debug, "Configured queue " << getName() << " with qpid.trace.id='" << traceId 
             << "' and qpid.trace.exclude='"<< excludeList << "' i.e. " << traceExclude.size() << " elements");

    FieldTable::ValuePtr p =_settings.get(qpidInsertSequenceNumbers);
    if (p && p->convertsTo<std::string>()) insertSequenceNumbers(p->get<std::string>());

    if (mgmtObject != 0)
        mgmtObject->set_arguments(ManagementAgent::toMap(_settings));

    if ( isDurable() && ! getPersistenceId() && ! recovering )
      store->create(*this, _settings);
}

void Queue::destroy()
{
    if (alternateExchange.get()) {
        Mutex::ScopedLock locker(messageLock);
        while(!messages.empty()){
            DeliverableMessage msg(getFront().payload);
            alternateExchange->route(msg, msg.getMessage().getRoutingKey(),
                                     msg.getMessage().getApplicationHeaders());
            popAndDequeue();
        }
        alternateExchange->decAlternateUsers();
    }

    if (store) {
        barrier.destroy();
        store->flush(*this);
        store->destroy(*this);
        store = 0;//ensure we make no more calls to the store for this queue
    }
    if (broker) broker->getCluster().destroy(*this);
}

void Queue::notifyDeleted()
{
    QueueListeners::ListenerSet set;
    {
        Mutex::ScopedLock locker(messageLock);
        listeners.snapshot(set);
        deleted = true;
    }
    set.notifyAll();
}

void Queue::bound(const string& exchange, const string& key,
                  const FieldTable& args)
{
    bindings.add(exchange, key, args);
}

void Queue::unbind(ExchangeRegistry& exchanges, Queue::shared_ptr shared_ref)
{
    bindings.unbind(exchanges, shared_ref);
}

void Queue::setPolicy(std::auto_ptr<QueuePolicy> _policy)
{
    policy = _policy;
}

const QueuePolicy* Queue::getPolicy()
{
    return policy.get();
}

uint64_t Queue::getPersistenceId() const 
{ 
    return persistenceId; 
}

void Queue::setPersistenceId(uint64_t _persistenceId) const
{
    if (mgmtObject != 0 && persistenceId == 0 && externalQueueStore)
    {
        ManagementObject* childObj = externalQueueStore->GetManagementObject();
        if (childObj != 0)
            childObj->setReference(mgmtObject->getObjectId());
    }
    persistenceId = _persistenceId;
}

void Queue::encode(Buffer& buffer) const 
{
    buffer.putShortString(name);
    buffer.put(settings);
    if (policy.get()) { 
        buffer.put(*policy);
    }
    buffer.putShortString(alternateExchange.get() ? alternateExchange->getName() : std::string(""));
}

uint32_t Queue::encodedSize() const
{
    return name.size() + 1/*short string size octet*/
        + (alternateExchange.get() ? alternateExchange->getName().size() : 0) + 1 /* short string */
        + settings.encodedSize()
        + (policy.get() ? (*policy).encodedSize() : 0);
}

Queue::shared_ptr Queue::decode ( QueueRegistry& queues, Buffer& buffer, bool recovering )
{
    string name;
    buffer.getShortString(name);
    std::pair<Queue::shared_ptr, bool> result = queues.declare(name, true);
    buffer.get(result.first->settings);
    result.first->configure(result.first->settings, recovering );
    if (result.first->policy.get() && buffer.available() >= result.first->policy->encodedSize()) {
        buffer.get ( *(result.first->policy) );
    }
    if (buffer.available()) {
        string altExch;
        buffer.getShortString(altExch);
        result.first->alternateExchangeName.assign(altExch);
    }

    return result.first;
}


void Queue::setAlternateExchange(boost::shared_ptr<Exchange> exchange)
{
    alternateExchange = exchange;
    if (mgmtObject) {
        if (exchange.get() != 0)
            mgmtObject->set_altExchange(exchange->GetManagementObject()->getObjectId());
        else
            mgmtObject->clr_altExchange();
    }
}

boost::shared_ptr<Exchange> Queue::getAlternateExchange()
{
    return alternateExchange;
}

void Queue::tryAutoDelete(Broker& broker, Queue::shared_ptr queue)
{
    if (broker.getQueues().destroyIf(queue->getName(), 
                                     boost::bind(boost::mem_fn(&Queue::canAutoDelete), queue))) {
        queue->unbind(broker.getExchanges(), queue);
        queue->destroy();
    }
}

bool Queue::isExclusiveOwner(const OwnershipToken* const o) const 
{ 
    Mutex::ScopedLock locker(ownershipLock);
    return o == owner; 
}

void Queue::releaseExclusiveOwnership() 
{ 
    Mutex::ScopedLock locker(ownershipLock);
    owner = 0; 
}

bool Queue::setExclusiveOwner(const OwnershipToken* const o) 
{ 
    Mutex::ScopedLock locker(ownershipLock);
    if (owner) {
        return false;
    } else {
        owner = o; 
        return true;
    }
}

bool Queue::hasExclusiveOwner() const 
{ 
    Mutex::ScopedLock locker(ownershipLock);
    return owner != 0; 
}

bool Queue::hasExclusiveConsumer() const 
{ 
    return exclusive; 
}

void Queue::setExternalQueueStore(ExternalQueueStore* inst) {
    if (externalQueueStore!=inst && externalQueueStore) 
        delete externalQueueStore; 
    externalQueueStore = inst;

    if (inst) {
        ManagementObject* childObj = inst->GetManagementObject();
        if (childObj != 0 && mgmtObject != 0)
            childObj->setReference(mgmtObject->getObjectId());
    }
}

ManagementObject* Queue::GetManagementObject (void) const
{
    return (ManagementObject*) mgmtObject;
}

Manageable::status_t Queue::ManagementMethod (uint32_t methodId, Args& args, string& etext)
{
    Manageable::status_t status = Manageable::STATUS_UNKNOWN_METHOD;

    QPID_LOG (debug, "Queue::ManagementMethod [id=" << methodId << "]");

    switch (methodId) {
    case _qmf::Queue::METHOD_PURGE :
        {
            _qmf::ArgsQueuePurge& purgeArgs = (_qmf::ArgsQueuePurge&) args;
            purge(purgeArgs.i_request);
            status = Manageable::STATUS_OK;
        }
        break;

    case _qmf::Queue::METHOD_REROUTE :
        {
            _qmf::ArgsQueueReroute& rerouteArgs = (_qmf::ArgsQueueReroute&) args;
            boost::shared_ptr<Exchange> dest;
            if (rerouteArgs.i_useAltExchange)
                dest = alternateExchange;
            else {
                try {
                    dest = broker->getExchanges().get(rerouteArgs.i_exchange);
                } catch(const std::exception&) {
                    status = Manageable::STATUS_PARAMETER_INVALID;
                    etext = "Exchange not found";
                    break;
                }
            }

            purge(rerouteArgs.i_request, dest);
            status = Manageable::STATUS_OK;
        }
        break;
    }

    return status;
}

void Queue::setPosition(SequenceNumber n) {
    Mutex::ScopedLock locker(messageLock);
    sequence = n;
}

SequenceNumber Queue::getPosition() {
    return sequence;
}

int Queue::getEventMode() { return eventMode; }

void Queue::setQueueEventManager(QueueEvents& mgr)
{
    eventMgr = &mgr;
}

void Queue::recoveryComplete(ExchangeRegistry& exchanges)
{
    // set the alternate exchange
    if (!alternateExchangeName.empty()) {
        try {
            Exchange::shared_ptr ae = exchanges.get(alternateExchangeName);
            setAlternateExchange(ae);
        } catch (const NotFoundException&) {
            QPID_LOG(warning, "Could not set alternate exchange \"" << alternateExchangeName << "\" on queue \"" << name << "\": exchange does not exist.");
        }
    }
    //process any pending dequeues
    for_each(pendingDequeues.begin(), pendingDequeues.end(), boost::bind(&Queue::dequeue, this, (TransactionContext*) 0, _1));
    pendingDequeues.clear();
}

void Queue::insertSequenceNumbers(const std::string& key)
{
    seqNoKey = key;
    insertSeqNo = !seqNoKey.empty();
    QPID_LOG(debug, "Inserting sequence numbers as " << key);
}

void Queue::enqueued(const QueuedMessage& m)
{
    if (m.payload) {
        if (policy.get()) {
            policy->recoverEnqueued(m.payload);
            policy->enqueued(m);
        }
        mgntEnqStats(m.payload);
        boost::intrusive_ptr<Message> payload = m.payload;
        enqueue ( 0, payload, true );
    } else {
        QPID_LOG(warning, "Queue informed of enqueued message that has no payload");
    }
}

bool Queue::isEnqueued(const QueuedMessage& msg)
{
    return !policy.get() || policy->isEnqueued(msg);
}

QueueListeners& Queue::getListeners() { return listeners; }

void Queue::checkNotDeleted()
{
    if (deleted) {
        throw ResourceDeletedException(QPID_MSG("Queue " << getName() << " has been deleted."));
    }
}

void Queue::flush()
{
    ScopedUse u(barrier);
    if (u.acquired && store) store->flush(*this);
}

Queue::UsageBarrier::UsageBarrier(Queue& q) : parent(q), count(0) {}

bool Queue::UsageBarrier::acquire()
{
    Monitor::ScopedLock l(parent.messageLock);
    if (parent.deleted) {
        return false;
    } else {
        ++count;
        return true;
    }
}

void Queue::UsageBarrier::release()
{
    Monitor::ScopedLock l(parent.messageLock);
    if (--count == 0) parent.messageLock.notifyAll();
}

void Queue::UsageBarrier::destroy()
{
    Monitor::ScopedLock l(parent.messageLock);
    parent.deleted = true;
    while (count) parent.messageLock.wait();
}
