#ifndef QPID_MESSAGING_MESSAGE_H
#define QPID_MESSAGING_MESSAGE_H

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

#include <string>
#include "qpid/Exception.h"
#include "qpid/messaging/Duration.h"
#include "qpid/types/Variant.h"
#include "qpid/messaging/ImportExport.h"

namespace qpid {
namespace messaging {

class Address;
class Codec;
struct MessageImpl;

/**
 * Representation of a message.
 */
class Message
{
  public:
    QPID_CLIENT_EXTERN Message(const std::string& bytes = std::string());
    QPID_CLIENT_EXTERN Message(const char*, size_t);
    QPID_CLIENT_EXTERN Message(const Message&);
    QPID_CLIENT_EXTERN ~Message();

    QPID_CLIENT_EXTERN Message& operator=(const Message&);

    QPID_CLIENT_EXTERN void setReplyTo(const Address&);
    QPID_CLIENT_EXTERN const Address& getReplyTo() const;

    QPID_CLIENT_EXTERN void setSubject(const std::string&);
    QPID_CLIENT_EXTERN const std::string& getSubject() const;

    QPID_CLIENT_EXTERN void setContentType(const std::string&);
    QPID_CLIENT_EXTERN const std::string& getContentType() const;

    QPID_CLIENT_EXTERN void setMessageId(const std::string&);
    QPID_CLIENT_EXTERN const std::string& getMessageId() const;

    QPID_CLIENT_EXTERN void setUserId(const std::string&);
    QPID_CLIENT_EXTERN const std::string& getUserId() const;

    QPID_CLIENT_EXTERN void setCorrelationId(const std::string&);
    QPID_CLIENT_EXTERN const std::string& getCorrelationId() const;

    QPID_CLIENT_EXTERN void setPriority(uint8_t);
    QPID_CLIENT_EXTERN uint8_t getPriority() const;

    /**
     * Set the time to live for this message in milliseconds.
     */
    QPID_CLIENT_EXTERN void setTtl(Duration ttl);
    /**
     *Get the time to live for this message in milliseconds.
     */
    QPID_CLIENT_EXTERN Duration getTtl() const;

    QPID_CLIENT_EXTERN void setDurable(bool durable);
    QPID_CLIENT_EXTERN bool getDurable() const;

    QPID_CLIENT_EXTERN bool getRedelivered() const;
    QPID_CLIENT_EXTERN void setRedelivered(bool);

    QPID_CLIENT_EXTERN const qpid::types::Variant::Map& getProperties() const;
    QPID_CLIENT_EXTERN qpid::types::Variant::Map& getProperties();

    QPID_CLIENT_EXTERN void setContent(const std::string&);
    /**
     * Note that chars are copied.
     */
    QPID_CLIENT_EXTERN void setContent(const char* chars, size_t count);

    /** Get the content as a std::string */
    QPID_CLIENT_EXTERN std::string getContent() const;
    /** Get a const pointer to the start of the content data. */
    QPID_CLIENT_EXTERN const char* getContentPtr() const;
    /** Get the size of content in bytes. */
    QPID_CLIENT_EXTERN size_t getContentSize() const;
  private:
    MessageImpl* impl;
    friend struct MessageImplAccess;
};

struct EncodingException : qpid::Exception
{
    EncodingException(const std::string& msg);
};

/**
 * Decodes message content into a Variant::Map.
 * 
 * @param message the message whose content should be decoded
 * @param map the map into which the message contents will be decoded
 * @param encoding if specified, the encoding to use - this overrides
 * any encoding specified by the content-type of the message
 * @exception EncodingException
 */
QPID_CLIENT_EXTERN void decode(const Message& message,
                               qpid::types::Variant::Map& map,
                               const std::string& encoding = std::string());
/**
 * Decodes message content into a Variant::List.
 * 
 * @param message the message whose content should be decoded
 * @param list the list into which the message contents will be decoded
 * @param encoding if specified, the encoding to use - this overrides
 * any encoding specified by the content-type of the message
 * @exception EncodingException
 */
QPID_CLIENT_EXTERN void decode(const Message& message,
                               qpid::types::Variant::List& list,
                               const std::string& encoding = std::string());
/**
 * Encodes a Variant::Map into a message.
 * 
 * @param map the map to be encoded
 * @param message the message whose content should be set to the encoded map
 * @param encoding if specified, the encoding to use - this overrides
 * any encoding specified by the content-type of the message
 * @exception EncodingException
 */
QPID_CLIENT_EXTERN void encode(const qpid::types::Variant::Map& map,
                               Message& message,
                               const std::string& encoding = std::string());
/**
 * Encodes a Variant::List into a message.
 * 
 * @param list the list to be encoded
 * @param message the message whose content should be set to the encoded list
 * @param encoding if specified, the encoding to use - this overrides
 * any encoding specified by the content-type of the message
 * @exception EncodingException
 */
QPID_CLIENT_EXTERN void encode(const qpid::types::Variant::List& list,
                               Message& message,
                               const std::string& encoding = std::string());

}} // namespace qpid::messaging

#endif  /*!QPID_MESSAGING_MESSAGE_H*/
