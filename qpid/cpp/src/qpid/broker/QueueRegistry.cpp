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
#include "qpid/broker/Queue.h"
#include "qpid/broker/QueueRegistry.h"
#include "qpid/broker/QueueEvents.h"
#include "qpid/broker/Exchange.h"
#include "qpid/log/Statement.h"
#include "qpid/framing/reply_exceptions.h"
#include <sstream>
#include <assert.h>

using namespace qpid::broker;
using namespace qpid::sys;
using std::string;

QueueRegistry::QueueRegistry(Broker* b) :
    counter(1), store(0), events(0), parent(0), lastNode(false), broker(b) {}

QueueRegistry::~QueueRegistry(){}

std::pair<Queue::shared_ptr, bool>
QueueRegistry::declare(const string& declareName, bool durable, 
                       bool autoDelete, const OwnershipToken* owner,
                       boost::shared_ptr<Exchange> alternate,
                       const qpid::framing::FieldTable& arguments,
                       bool recovering/*true if this declare is a
                                        result of recovering queue
                                        definition from persistente
                                        record*/)
{
    Queue::shared_ptr queue;
    std::pair<Queue::shared_ptr, bool> result;
    {
        RWlock::ScopedWlock locker(lock);
        string name = declareName.empty() ? generateName() : declareName;
        assert(!name.empty());
        QueueMap::iterator i =  queues.find(name);

        if (i == queues.end()) {
            queue.reset(new Queue(name, autoDelete, durable ? store : 0, owner, parent, broker));
            if (alternate) {
                queue->setAlternateExchange(alternate);//need to do this *before* create
                alternate->incAlternateUsers();
            }
            if (!recovering) {
                //apply settings & create persistent record if required
                queue->create(arguments);
            } else {
                //i.e. recovering a queue for which we already have a persistent record
                queue->configure(arguments);
            }
            queues[name] = queue;
            if (lastNode) queue->setLastNodeFailure();
            result = std::pair<Queue::shared_ptr, bool>(queue, true);
        } else {
            result = std::pair<Queue::shared_ptr, bool>(i->second, false);
        }
    }
    if (broker && queue) broker->getConfigurationObservers().queueCreate(queue);
    return result;
}

void QueueRegistry::destroy(const string& name) {
    Queue::shared_ptr q;
    {
        qpid::sys::RWlock::ScopedWlock locker(lock);
        QueueMap::iterator i = queues.find(name);
        if (i != queues.end()) {
            Queue::shared_ptr q = i->second;
            queues.erase(i);
        }
    }
    if (broker && q) broker->getConfigurationObservers().queueDestroy(q);
}

Queue::shared_ptr QueueRegistry::find(const string& name){
    RWlock::ScopedRlock locker(lock);
    QueueMap::iterator i = queues.find(name);
    if (i == queues.end()) {
        return Queue::shared_ptr();
    } else {
        return i->second;
    }
}

Queue::shared_ptr QueueRegistry::get(const string& name) {
    Queue::shared_ptr q = find(name);
    if (!q) throw framing::NotFoundException(QPID_MSG("Queue not found: "<<name));
    return q;
}

string QueueRegistry::generateName(){
    string name;
    do {
        std::stringstream ss;
        ss << "tmp_" << counter++;
        name = ss.str();
        // Thread safety: Private function, only called with lock held
        // so this is OK.
    } while(queues.find(name) != queues.end());
    return name;
}

void QueueRegistry::setStore (MessageStore* _store)
{
    store = _store;
}

MessageStore* QueueRegistry::getStore() const {
    return store;
}

void QueueRegistry::updateQueueClusterState(bool _lastNode)
{
    RWlock::ScopedRlock locker(lock);
    for (QueueMap::iterator i = queues.begin(); i != queues.end(); i++) {
        if (_lastNode){
            i->second->setLastNodeFailure();
        } else {
            i->second->clearLastNodeFailure();
        }
    }
    lastNode = _lastNode;
}
