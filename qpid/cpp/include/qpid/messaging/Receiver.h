#ifndef QPID_MESSAGING_RECEIVER_H
#define QPID_MESSAGING_RECEIVER_H

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
#include "qpid/messaging/exceptions.h"
#include "qpid/messaging/ImportExport.h"
#include "qpid/messaging/Handle.h"
#include "qpid/messaging/Duration.h"

namespace qpid {
namespace messaging {

template <class> class PrivateImplRef;

class Message;
class ReceiverImpl;
class Session;

/**
 * Interface through which messages are received.
 */
class Receiver : public qpid::messaging::Handle<ReceiverImpl>
{
  public:
    QPID_CLIENT_EXTERN Receiver(ReceiverImpl* impl = 0);
    QPID_CLIENT_EXTERN Receiver(const Receiver&);
    QPID_CLIENT_EXTERN ~Receiver();
    QPID_CLIENT_EXTERN Receiver& operator=(const Receiver&);
    /**
     * Retrieves a message from this receivers local queue, or waits
     * for upto the specified timeout for a message to become
     * available. Returns false if there is no message to give after
     * waiting for the specified timeout.
     */
    QPID_CLIENT_EXTERN bool get(Message& message, Duration timeout=Duration::FOREVER);
    /**
     * Retrieves a message from this receivers local queue, or waits
     * for up to the specified timeout for a message to become
     * available.
     *
     * @exception NoMessageAvailable if there is no message to give
     * after waiting for the specified timeout.
     */
    QPID_CLIENT_EXTERN Message get(Duration timeout=Duration::FOREVER);
    /**
     * Retrieves a message for this receivers subscription or waits
     * for up to the specified timeout for one to become
     * available. Unlike get() this method will check with the server
     * that there is no message for the subscription this receiver is
     * serving before returning false.
     */
    QPID_CLIENT_EXTERN bool fetch(Message& message, Duration timeout=Duration::FOREVER);
    /**
     * Retrieves a message for this receivers subscription or waits
     * for up to the specified timeout for one to become
     * available. Unlike get() this method will check with the server
     * that there is no message for the subscription this receiver is
     * serving before throwing an exception.
     *
     * @exception NoMessageAvailable if there is no message to give
     * after waiting for the specified timeout.
     */
    QPID_CLIENT_EXTERN Message fetch(Duration timeout=Duration::FOREVER);
    /**
     * Sets the capacity for the receiver. The capacity determines how
     * many incoming messages can be held in the receiver before being
     * requested by a client via fetch() (or pushed to a listener).
     */
    QPID_CLIENT_EXTERN void setCapacity(uint32_t);
    /**
     * Returns the capacity of the receiver. The capacity determines
     * how many incoming messages can be held in the receiver before
     * being requested by a client via fetch() (or pushed to a
     * listener).
     */
    QPID_CLIENT_EXTERN uint32_t getCapacity();
    /**
     * Returns the number of messages received and waiting to be
     * fetched.
     */
    QPID_CLIENT_EXTERN uint32_t getAvailable();
    /**
     * Returns a count of the number of messages received on this
     * receiver that have been acknowledged, but for which that
     * acknowledgement has not yet been confirmed as processed by the
     * server.
     */
    QPID_CLIENT_EXTERN uint32_t getPendingAck();

    /**
     * Cancels this receiver.
     */
    QPID_CLIENT_EXTERN void close();

    /**
     * Returns the name of this receiver.
     */
    QPID_CLIENT_EXTERN const std::string& getName() const;

    /**
     * Returns a handle to the session associated with this receiver.
     */
    QPID_CLIENT_EXTERN Session getSession() const;

  private:
  friend class qpid::messaging::PrivateImplRef<Receiver>;
};
}} // namespace qpid::messaging

#endif  /*!QPID_MESSAGING_RECEIVER_H*/
