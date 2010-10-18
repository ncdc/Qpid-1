#ifndef QPID_BROKER_NULLCLUSTER_H
#define QPID_BROKER_NULLCLUSTER_H

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

#include <qpid/broker/Cluster.h>

namespace qpid {
namespace broker {

/**
 * No-op implementation of Cluster interface, installed by broker when
 * no cluster plug-in is present or clustering is disabled.
 */
class NullCluster : public Cluster
{
  public:

    // Messages

    virtual void routing(const boost::intrusive_ptr<Message>&) {}
    virtual void enqueue(QueuedMessage&) {}
    virtual void routed(const boost::intrusive_ptr<Message>&) {}
    virtual void acquire(const QueuedMessage&) {}
    virtual void accept(const QueuedMessage&) {}
    virtual void reject(const QueuedMessage&) {}
    virtual void rejected(const QueuedMessage&) {}
    virtual void release(const QueuedMessage&) {}
    virtual void dequeue(const QueuedMessage&) {}

    // Consumers

    virtual void consume(const Queue&, size_t) {}
    virtual void cancel(const Queue&, size_t) {}

    // Wiring

    virtual void create(const Queue&) {}
    virtual void destroy(const Queue&) {}
    virtual void create(const Exchange&) {}
    virtual void destroy(const Exchange&) {}
    virtual void bind(const Queue&, const Exchange&, const std::string&, const framing::FieldTable&) {}
};

}} // namespace qpid::broker

#endif
