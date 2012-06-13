#ifndef _broker_Message_h
#define _broker_Message_h

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

#include "qpid/broker/BrokerImportExport.h"
#include "qpid/broker/PersistableMessage.h"
#include "qpid/broker/MessageAdapter.h"
#include "qpid/framing/amqp_types.h"
#include "qpid/sys/Monitor.h"
#include "qpid/sys/Time.h"
#include <boost/function.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <memory>
#include <string>
#include <vector>

namespace qpid {

namespace framing {
class AMQBody;
class AMQHeaderBody;
class FieldTable;
class SequenceNumber;
}

namespace broker {
class ConnectionToken;
class Exchange;
class ExchangeRegistry;
class MessageStore;
class Queue;
class ExpiryPolicy;

class Message : public PersistableMessage {
public:
    typedef boost::function<void (const boost::intrusive_ptr<Message>&)> MessageCallback;

    QPID_BROKER_EXTERN Message(const framing::SequenceNumber& id = framing::SequenceNumber());
    QPID_BROKER_EXTERN ~Message();

    uint64_t getPersistenceId() const { return persistenceId; }
    void setPersistenceId(uint64_t _persistenceId) const { persistenceId = _persistenceId; }

    bool getRedelivered() const { return redelivered; }
    void redeliver() { redelivered = true; }

    const ConnectionToken* getPublisher() const {  return publisher; }
    void setPublisher(ConnectionToken* p) {  publisher = p; }

    const framing::SequenceNumber& getCommandId() { return frames.getId(); }

    QPID_BROKER_EXTERN uint64_t contentSize() const;

    QPID_BROKER_EXTERN std::string getRoutingKey() const;
    const boost::shared_ptr<Exchange> getExchange(ExchangeRegistry&) const;
    QPID_BROKER_EXTERN std::string getExchangeName() const;
    bool isImmediate() const;
    QPID_BROKER_EXTERN const framing::FieldTable* getApplicationHeaders() const;
    QPID_BROKER_EXTERN std::string getAppId() const;
    QPID_BROKER_EXTERN bool isPersistent() const;
    bool requiresAccept();

    /** determine msg expiration time using the TTL value if present */
    QPID_BROKER_EXTERN void computeExpiration(const boost::intrusive_ptr<ExpiryPolicy>& e);
    void setExpiryPolicy(const boost::intrusive_ptr<ExpiryPolicy>& e);
    bool hasExpired();
    sys::AbsTime getExpiration() const { return expiration; }
    void setExpiration(sys::AbsTime exp) { expiration = exp; }
    void adjustTtl();
    void setRedelivered();
    QPID_BROKER_EXTERN void insertCustomProperty(const std::string& key, int64_t value);
    QPID_BROKER_EXTERN void insertCustomProperty(const std::string& key, const std::string& value);
    QPID_BROKER_EXTERN void removeCustomProperty(const std::string& key);
    void setExchange(const std::string&);
    void clearApplicationHeadersFlag();
    /** set the timestamp delivery property to the current time-of-day */
    QPID_BROKER_EXTERN void setTimestamp();

    framing::FrameSet& getFrames() { return frames; }
    const framing::FrameSet& getFrames() const { return frames; }

    template <class T> const T* getProperties() const {
        const qpid::framing::AMQHeaderBody* p = frames.getHeaders();
        return p->get<T>();
    }

    template <class T> const T* hasProperties() const {
        const qpid::framing::AMQHeaderBody* p = frames.getHeaders();
        return p->get<T>();
    }

    template <class T> void eraseProperties() {
        qpid::framing::AMQHeaderBody* p = frames.getHeaders();
        p->erase<T>();
    }

    template <class T> const T* getMethod() const {
        return frames.as<T>();
    }

    template <class T> T* getMethod() {
        return frames.as<T>();
    }

    template <class T> bool isA() const {
        return frames.isA<T>();
    }

    uint32_t getRequiredCredit();

    void encode(framing::Buffer& buffer) const;
    void encodeContent(framing::Buffer& buffer) const;

    /**
     * @returns the size of the buffer needed to encode this
     * message in its entirety
     */
    uint32_t encodedSize() const;
    /**
     * @returns the size of the buffer needed to encode the
     * 'header' of this message (not just the header frame,
     * but other meta data e.g.routing key and exchange)
     */
    uint32_t encodedHeaderSize() const;
    uint32_t encodedContentSize() const;

    QPID_BROKER_EXTERN void decodeHeader(framing::Buffer& buffer);
    QPID_BROKER_EXTERN void decodeContent(framing::Buffer& buffer);

    void QPID_BROKER_EXTERN tryReleaseContent();
    void releaseContent();
    void releaseContent(MessageStore* s);//deprecated, use 'setStore(store); releaseContent();' instead
    void destroy();

    bool getContentFrame(const Queue& queue, framing::AMQFrame& frame, uint16_t maxContentSize, uint64_t offset) const;
    QPID_BROKER_EXTERN void sendContent(const Queue& queue, framing::FrameHandler& out, uint16_t maxFrameSize) const;
    void sendHeader(framing::FrameHandler& out, uint16_t maxFrameSize) const;

    QPID_BROKER_EXTERN bool isContentLoaded() const;

    bool isExcluded(const std::vector<std::string>& excludes) const;
    void addTraceId(const std::string& id);
    void clearTrace();

    void forcePersistent();
    bool isForcedPersistent();

    /** Call cb when dequeue is complete, may call immediately. Holds cb by reference. */
    void setDequeueCompleteCallback(MessageCallback& cb);
    void resetDequeueCompleteCallback();

    uint8_t getPriority() const;
    bool getIsManagementMessage() const;
    void setIsManagementMessage(bool b);
  private:
    MessageAdapter& getAdapter() const;
    void allDequeuesComplete();

    mutable sys::Mutex lock;
    framing::FrameSet frames;
    mutable boost::shared_ptr<Exchange> exchange;
    mutable uint64_t persistenceId;
    bool redelivered;
    bool loaded;
    bool staged;
    bool forcePersistentPolicy; // used to force message as durable, via a broker policy
    ConnectionToken* publisher;
    mutable MessageAdapter* adapter;
    qpid::sys::AbsTime expiration;
    boost::intrusive_ptr<ExpiryPolicy> expiryPolicy;

    static TransferAdapter TRANSFER;

    mutable boost::intrusive_ptr<Message> empty;

    sys::Monitor callbackLock;
    MessageCallback* dequeueCallback;
    bool inCallback;

    uint32_t requiredCredit;
    bool isManagementMessage;
      mutable bool copyHeaderOnWrite;

    /**
     * Expects lock to be held
     */
    template <class T> T* getModifiableProperties() {
        return getHeaderBody()->get<T>(true);
    }
    qpid::framing::AMQHeaderBody* getHeaderBody();
};

}}


#endif
