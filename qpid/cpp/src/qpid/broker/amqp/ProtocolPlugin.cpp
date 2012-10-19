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
#include "qpid/Plugin.h"
#include "qpid/SaslFactory.h"
#include "qpid/NullSaslServer.h"
#include "qpid/broker/Broker.h"
#include "qpid/broker/Message.h"
#include "qpid/broker/Protocol.h"
#include "qpid/broker/RecoverableMessage.h"
#include "qpid/broker/RecoverableMessageImpl.h"
#include "qpid/broker/amqp/Connection.h"
#include "qpid/broker/amqp/Message.h"
#include "qpid/broker/amqp/Sasl.h"
#include "qpid/broker/amqp/Translation.h"
#include "qpid/broker/amqp_0_10/MessageTransfer.h"
#include "qpid/framing/Buffer.h"
#include "qpid/framing/ProtocolVersion.h"
#include "qpid/sys/ConnectionCodec.h"
#include "qpid/log/Statement.h"

namespace qpid {
namespace broker {
namespace amqp {

class ProtocolImpl : public Protocol
{
  public:
    ProtocolImpl(Broker& b) : broker(b) {}
    qpid::sys::ConnectionCodec* create(const qpid::framing::ProtocolVersion&, qpid::sys::OutputControl&, const std::string&, const qpid::sys::SecuritySettings&);
    boost::intrusive_ptr<const qpid::broker::amqp_0_10::MessageTransfer> translate(const qpid::broker::Message&);
    boost::shared_ptr<RecoverableMessage> recover(qpid::framing::Buffer&);
  private:
    Broker& broker;
};

struct ProtocolPlugin : public Plugin
{
    void earlyInitialize(Plugin::Target& target)
    {
        //need to register protocol before recovery from store
        broker::Broker* broker = dynamic_cast<qpid::broker::Broker*>(&target);
        if (broker) {
            broker->getProtocolRegistry().add("AMQP 1.0", new ProtocolImpl(*broker));
        }
    }

    void initialize(Plugin::Target&) {}
};

ProtocolPlugin instance; // Static initialization

qpid::sys::ConnectionCodec* ProtocolImpl::create(const qpid::framing::ProtocolVersion& v, qpid::sys::OutputControl& out, const std::string& id, const qpid::sys::SecuritySettings& external)
{
    if (v == qpid::framing::ProtocolVersion(1, 0)) {
        if (v.getProtocol() == qpid::framing::ProtocolVersion::SASL) {
            if (broker.getOptions().auth) {
                QPID_LOG(info, "Using AMQP 1.0 (with SASL layer)");
                return new qpid::broker::amqp::Sasl(out, id, broker, qpid::SaslFactory::getInstance().createServer(broker.getOptions().realm, broker.getOptions().requireEncrypted, external));
            } else {
                std::auto_ptr<SaslServer> authenticator(new qpid::NullSaslServer(broker.getOptions().realm));
                QPID_LOG(info, "Using AMQP 1.0 (with dummy SASL layer)");
                return new qpid::broker::amqp::Sasl(out, id, broker, authenticator);
            }
        } else {
            if (broker.getOptions().auth) {
                throw qpid::Exception("SASL layer required!");
            } else {
                QPID_LOG(info, "Using AMQP 1.0 (no SASL layer)");
                return new qpid::broker::amqp::Connection(out, id, broker, false);
            }
        }
    }
    return 0;
}

boost::intrusive_ptr<const qpid::broker::amqp_0_10::MessageTransfer> ProtocolImpl::translate(const qpid::broker::Message& m)
{
    qpid::broker::amqp::Translation t(m);
    return t.getTransfer();
}

boost::shared_ptr<RecoverableMessage> ProtocolImpl::recover(qpid::framing::Buffer& buffer)
{
    QPID_LOG(debug, "Recovering, checking for 1.0 message format indicator...");
    uint32_t format = buffer.getLong();
    if (format == 0) {
        QPID_LOG(debug, "Recovered message IS in 1.0 format");
        //this is a 1.0 format message
        boost::intrusive_ptr<qpid::broker::amqp::Message> m(new qpid::broker::amqp::Message(buffer.available()));
        m->decodeHeader(buffer);
        return RecoverableMessage::shared_ptr(new RecoverableMessageImpl(qpid::broker::Message(m, m)));
    } else {
        QPID_LOG(debug, "Recovered message is NOT in 1.0 format");
        return RecoverableMessage::shared_ptr();
    }
}


}}} // namespace qpid::broker::amqp
