#ifndef QPID_BROKER_SEMANTICSTATE_H
#define QPID_BROKER_SEMANTICSTATE_H

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
#include "qpid/broker/Consumer.h"
#include "qpid/broker/Credit.h"
#include "qpid/broker/Deliverable.h"
#include "qpid/broker/DeliveryAdapter.h"
#include "qpid/broker/DeliveryRecord.h"
#include "qpid/broker/DtxBuffer.h"
#include "qpid/broker/DtxManager.h"
#include "qpid/broker/NameGenerator.h"
#include "qpid/broker/QueueObserver.h"
#include "qpid/broker/TxBuffer.h"

#include "qpid/framing/FrameHandler.h"
#include "qpid/framing/SequenceSet.h"
#include "qpid/framing/Uuid.h"
#include "qpid/sys/AggregateOutput.h"
#include "qpid/sys/Mutex.h"
#include "qpid/sys/AtomicValue.h"
#include "qmf/org/apache/qpid/broker/Subscription.h"

#include <list>
#include <map>
#include <vector>

#include <boost/enable_shared_from_this.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/cast.hpp>

namespace qpid {
namespace broker {

class SessionContext;

/**
 *
 * SemanticState implements the behavior of a Session, especially the
 * state of consumers subscribed to queues. The code for ConsumerImpl
 * is also in SemanticState.cpp
 *
 * SemanticState holds the AMQP Execution and Model state of an open
 * session, whether attached to a channel or suspended. It is not
 * dependent on any specific AMQP version.
 *
 * Message delivery is driven by ConsumerImpl::doOutput(), which is
 * called when a client's socket is ready to write data.
 *
 */
class SemanticState : private boost::noncopyable {
  public:
    class ConsumerImpl : public Consumer, public sys::OutputTask,
                         public boost::enable_shared_from_this<ConsumerImpl>,
                         public management::Manageable
    {
      protected:
        mutable qpid::sys::Mutex lock;
        SemanticState* const parent;
      private:
        const boost::shared_ptr<Queue> queue;
        const bool ackExpected;
        const bool acquire;
        bool blocked;
        bool exclusive;
        std::string resumeId;
        const std::string tag;  // <destination> from AMQP 0-10 Message.subscribe command
        uint64_t resumeTtl;
        framing::FieldTable arguments;
        Credit credit;
        bool notifyEnabled;
        const int syncFrequency;
        int deliveryCount;
        qmf::org::apache::qpid::broker::Subscription* mgmtObject;

        bool checkCredit(boost::intrusive_ptr<Message>& msg);
        void allocateCredit(boost::intrusive_ptr<Message>& msg);
        bool haveCredit();

      protected:
        QPID_BROKER_EXTERN virtual bool doDispatch();
        size_t unacked() { return parent->unacked.size(); }

      public:
        typedef boost::shared_ptr<ConsumerImpl> shared_ptr;

        QPID_BROKER_EXTERN ConsumerImpl(
            SemanticState* parent,
            const std::string& name, boost::shared_ptr<Queue> queue,
            bool ack, bool acquire, bool exclusive,
            const std::string& tag, const std::string& resumeId, uint64_t resumeTtl,
            const framing::FieldTable& arguments);
        QPID_BROKER_EXTERN virtual ~ConsumerImpl();
        QPID_BROKER_EXTERN OwnershipToken* getSession();
        QPID_BROKER_EXTERN virtual bool deliver(QueuedMessage& msg);
        QPID_BROKER_EXTERN bool filter(boost::intrusive_ptr<Message> msg);
        QPID_BROKER_EXTERN bool accept(boost::intrusive_ptr<Message> msg);
        QPID_BROKER_EXTERN void cancel() {}

        QPID_BROKER_EXTERN void disableNotify();
        QPID_BROKER_EXTERN void enableNotify();
        QPID_BROKER_EXTERN void notify();
        QPID_BROKER_EXTERN bool isNotifyEnabled() const;

        QPID_BROKER_EXTERN void requestDispatch();

        QPID_BROKER_EXTERN void setWindowMode();
        QPID_BROKER_EXTERN void setCreditMode();
        QPID_BROKER_EXTERN void addByteCredit(uint32_t value);
        QPID_BROKER_EXTERN void addMessageCredit(uint32_t value);
        QPID_BROKER_EXTERN void flush();
        QPID_BROKER_EXTERN void stop();
        QPID_BROKER_EXTERN void complete(DeliveryRecord&);
        boost::shared_ptr<Queue> getQueue() const { return queue; }
        bool isBlocked() const { return blocked; }
        bool setBlocked(bool set) { std::swap(set, blocked); return set; }

        QPID_BROKER_EXTERN bool doOutput();

        Credit& getCredit() { return credit; }
        const Credit& getCredit() const { return credit; }
        bool isAckExpected() const { return ackExpected; }
        bool isAcquire() const { return acquire; }
        bool isExclusive() const { return exclusive; }
        std::string getResumeId() const { return resumeId; };
        const std::string& getTag() const { return tag; }
        uint64_t getResumeTtl() const { return resumeTtl; }
	uint32_t getDeliveryCount() const { return deliveryCount; }
	void setDeliveryCount(uint32_t _deliveryCount) { deliveryCount = _deliveryCount; }
        const framing::FieldTable& getArguments() const { return arguments; }

        SemanticState& getParent() { return *parent; }
        const SemanticState& getParent() const { return *parent; }

        void acknowledged(const broker::QueuedMessage&) {}

        // manageable entry points
        QPID_BROKER_EXTERN management::ManagementObject*
        GetManagementObject(void) const;

        QPID_BROKER_EXTERN management::Manageable::status_t
        ManagementMethod(uint32_t methodId, management::Args& args, std::string& text);
    };

    typedef std::map<std::string, DtxBuffer::shared_ptr> DtxBufferMap;

  private:
    typedef std::map<std::string, ConsumerImpl::shared_ptr> ConsumerImplMap;

    SessionContext& session;
    DeliveryAdapter& deliveryAdapter;
    ConsumerImplMap consumers;
    NameGenerator tagGenerator;
    DeliveryRecords unacked;
    TxBuffer::shared_ptr txBuffer;
    DtxBuffer::shared_ptr dtxBuffer;
    bool dtxSelected;
    DtxBufferMap suspendedXids;
    framing::SequenceSet accumulatedAck;
    boost::shared_ptr<Exchange> cacheExchange;
    const bool authMsg;
    const std::string userID;
    bool closeComplete;
    //needed for queue delete events in auto-delete:
    const std::string connectionId;

    void route(boost::intrusive_ptr<Message> msg, Deliverable& strategy);
    void checkDtxTimeout();

    bool complete(DeliveryRecord&);
    AckRange findRange(DeliveryId first, DeliveryId last);
    void requestDispatch();
    void cancel(ConsumerImpl::shared_ptr);
    void disable(ConsumerImpl::shared_ptr);

  public:

    SemanticState(DeliveryAdapter&, SessionContext&);
    ~SemanticState();

    SessionContext& getSession() { return session; }
    const SessionContext& getSession() const { return session; }

    const ConsumerImpl::shared_ptr find(const std::string& destination) const;
    bool find(const std::string& destination, ConsumerImpl::shared_ptr&) const;

    /**
     * Get named queue, never returns 0.
     * @return: named queue
     * @exception: ChannelException if no queue of that name is found.
     * @exception: ConnectionException if name="" and session has no default.
     */
    boost::shared_ptr<Queue> getQueue(const std::string& name) const;

    bool exists(const std::string& consumerTag);

    void consume(const std::string& destination,
                 boost::shared_ptr<Queue> queue,
                 bool ackRequired, bool acquire, bool exclusive,
                 const std::string& resumeId=std::string(), uint64_t resumeTtl=0,
                 const framing::FieldTable& = framing::FieldTable());

    bool cancel(const std::string& tag);

    void setWindowMode(const std::string& destination);
    void setCreditMode(const std::string& destination);
    void addByteCredit(const std::string& destination, uint32_t value);
    void addMessageCredit(const std::string& destination, uint32_t value);
    void flush(const std::string& destination);
    void stop(const std::string& destination);

    void startTx();
    void commit(MessageStore* const store);
    void rollback();
    void selectDtx();
    bool getDtxSelected() const { return dtxSelected; }
    void startDtx(const std::string& xid, DtxManager& mgr, bool join);
    void endDtx(const std::string& xid, bool fail);
    void suspendDtx(const std::string& xid);
    void resumeDtx(const std::string& xid);
    void recover(bool requeue);
    void deliver(DeliveryRecord& message, bool sync);
    void acquire(DeliveryId first, DeliveryId last, DeliveryIds& acquired);
    void release(DeliveryId first, DeliveryId last, bool setRedelivered);
    void reject(DeliveryId first, DeliveryId last);
    void handle(boost::intrusive_ptr<Message> msg);

    void completed(const framing::SequenceSet& commands);
    void accepted(const framing::SequenceSet& commands);

    void attached();
    void detached();
    void closed();

    // Used by cluster to re-create sessions
    template <class F> void eachConsumer(F f) {
        for(ConsumerImplMap::iterator i = consumers.begin(); i != consumers.end(); ++i)
            f(i->second);
    }
    DeliveryRecords& getUnacked() { return unacked; }
    framing::SequenceSet getAccumulatedAck() const { return accumulatedAck; }
    TxBuffer::shared_ptr getTxBuffer() const { return txBuffer; }
    DtxBuffer::shared_ptr getDtxBuffer() const { return dtxBuffer; }
    void setTxBuffer(const TxBuffer::shared_ptr& txb) { txBuffer = txb; }
    void setDtxBuffer(const DtxBuffer::shared_ptr& dtxb) { dtxBuffer = dtxb; txBuffer = dtxb; }
    void setAccumulatedAck(const framing::SequenceSet& s) { accumulatedAck = s; }
    void record(const DeliveryRecord& delivery);
    DtxBufferMap& getSuspendedXids() { return suspendedXids; }
};

}} // namespace qpid::broker




#endif  /*!QPID_BROKER_SEMANTICSTATE_H*/
