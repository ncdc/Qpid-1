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
#include "qpid/broker/Bridge.h"
#include "qpid/broker/FedOps.h"
#include "qpid/broker/ConnectionState.h"
#include "qpid/broker/Connection.h"
#include "qpid/broker/Link.h"
#include "qpid/broker/LinkRegistry.h"
#include "qpid/ha/BrokerReplicator.h"
#include "qpid/broker/SessionState.h"

#include "qpid/management/ManagementAgent.h"
#include "qpid/types/Variant.h"
#include "qpid/amqp_0_10/Codecs.h"
#include "qpid/framing/Uuid.h"
#include "qpid/framing/MessageProperties.h"
#include "qpid/framing/MessageTransferBody.h"
#include "qpid/log/Statement.h"
#include <iostream>

using qpid::framing::FieldTable;
using qpid::framing::Uuid;
using qpid::framing::Buffer;
using qpid::framing::AMQFrame;
using qpid::framing::AMQContentBody;
using qpid::framing::AMQHeaderBody;
using qpid::framing::MessageProperties;
using qpid::framing::MessageTransferBody;
using qpid::types::Variant;
using qpid::management::ManagementAgent;
using std::string;
namespace _qmf = qmf::org::apache::qpid::broker;

namespace qpid {
namespace broker {

void Bridge::PushHandler::handle(framing::AMQFrame& frame)
{
    conn->received(frame);
}

Bridge::Bridge(Link* _link, framing::ChannelId _id, CancellationListener l,
               const _qmf::ArgsLinkBridge& _args,
               InitializeCallback init) :
    link(_link), id(_id), args(_args), mgmtObject(0),
    listener(l), name(Uuid(true).str()), queueName("qpid.bridge_queue_"), persistenceId(0),
    initialize(init)
{
    std::stringstream title;
    title << id << "_" << name;
    queueName += title.str();
    ManagementAgent* agent = link->getBroker()->getManagementAgent();
    if (agent != 0) {
        mgmtObject = new _qmf::Bridge
            (agent, this, link, id, args.i_durable, args.i_src, args.i_dest,
             args.i_key, args.i_srcIsQueue, args.i_srcIsLocal,
             args.i_tag, args.i_excludes, args.i_dynamic, args.i_sync);
        agent->addObject(mgmtObject);
    }
    QPID_LOG(debug, "Bridge " << name << " created from " << args.i_src << " to " << args.i_dest);
}

Bridge::~Bridge()
{
    mgmtObject->resourceDestroy();
}

void Bridge::create(Connection& c)
{
    connState = &c;
    conn = &c;
    FieldTable options;
    if (args.i_sync) options.setInt("qpid.sync_frequency", args.i_sync);
    SessionHandler& sessionHandler = c.getChannel(id);
    if (args.i_srcIsLocal) {
        if (args.i_dynamic)
            throw Exception("Dynamic routing not supported for push routes");
        // Point the bridging commands at the local connection handler
        pushHandler.reset(new PushHandler(&c));
        channelHandler.reset(new framing::ChannelHandler(id, pushHandler.get()));

        session.reset(new framing::AMQP_ServerProxy::Session(*channelHandler));
        peer.reset(new framing::AMQP_ServerProxy(*channelHandler));

        session->attach(name, false);
        session->commandPoint(0,0);
    } else {
        sessionHandler.attachAs(name);
        // Point the bridging commands at the remote peer broker
        peer.reset(new framing::AMQP_ServerProxy(sessionHandler.out));
    }

    if (args.i_srcIsLocal) sessionHandler.getSession()->disableReceiverTracking();
    if (initialize) initialize(*this, sessionHandler);
    else if (args.i_srcIsQueue) {
        peer->getMessage().subscribe(args.i_src, args.i_dest, args.i_sync ? 0 : 1, 0, false, "", 0, options);
        peer->getMessage().flow(args.i_dest, 0, 0xFFFFFFFF);
        peer->getMessage().flow(args.i_dest, 1, 0xFFFFFFFF);
        QPID_LOG(debug, "Activated bridge " << name << " for route from queue " << args.i_src << " to " << args.i_dest);
    } else {
        FieldTable queueSettings;

        if (args.i_tag.size()) {
            queueSettings.setString("qpid.trace.id", args.i_tag);
        } else {
            const string& peerTag = c.getFederationPeerTag();
            if (peerTag.size())
                queueSettings.setString("qpid.trace.id", peerTag);
        }

        if (args.i_excludes.size()) {
            queueSettings.setString("qpid.trace.exclude", args.i_excludes);
        } else {
            const string& localTag = link->getBroker()->getFederationTag();
            if (localTag.size())
                queueSettings.setString("qpid.trace.exclude", localTag);
        }

        bool durable = false;//should this be an arg, or would we use srcIsQueue for durable queues?
        bool autoDelete = !durable;//auto delete transient queues?
        peer->getQueue().declare(queueName, "", false, durable, true, autoDelete, queueSettings);
        if (!args.i_dynamic)
            peer->getExchange().bind(queueName, args.i_src, args.i_key, FieldTable());
        peer->getMessage().subscribe(queueName, args.i_dest, 1, 0, false, "", 0, FieldTable());
        peer->getMessage().flow(args.i_dest, 0, 0xFFFFFFFF);
        peer->getMessage().flow(args.i_dest, 1, 0xFFFFFFFF);

        if (args.i_dynamic) {
            Exchange::shared_ptr exchange = link->getBroker()->getExchanges().get(args.i_src);
            if (exchange.get() == 0)
                throw Exception("Exchange not found for dynamic route");
            exchange->registerDynamicBridge(this);
            QPID_LOG(debug, "Activated bridge " << name << " for dynamic route for exchange " << args.i_src);
        } else {
            QPID_LOG(debug, "Activated bridge " << name << " for static route from exchange " << args.i_src << " to " << args.i_dest);
        }
    }
    if (args.i_srcIsLocal) sessionHandler.getSession()->enableReceiverTracking();
}

void Bridge::cancel(Connection&)
{
    if (resetProxy()) {
        peer->getMessage().cancel(args.i_dest);
        peer->getSession().detach(name);
    }
    QPID_LOG(debug, "Cancelled bridge " << name);
}

void Bridge::closed()
{
    if (args.i_dynamic) {
        Exchange::shared_ptr exchange = link->getBroker()->getExchanges().find(args.i_src);
        if (exchange.get()) exchange->removeDynamicBridge(this);
    }
    QPID_LOG(debug, "Closed bridge " << name);
}

void Bridge::destroy()
{
    listener(this);
}

bool Bridge::isSessionReady() const
{
    SessionHandler& sessionHandler = conn->getChannel(id);
    return sessionHandler.ready();
}

void Bridge::setPersistenceId(uint64_t pId) const
{
    persistenceId = pId;
}

Bridge::shared_ptr Bridge::decode(LinkRegistry& links, Buffer& buffer)
{
    string   host;
    uint16_t port;
    string   src;
    string   dest;
    string   key;
    string   id;
    string   excludes;

    buffer.getShortString(host);
    port = buffer.getShort();
    bool durable(buffer.getOctet());
    buffer.getShortString(src);
    buffer.getShortString(dest);
    buffer.getShortString(key);
    bool is_queue(buffer.getOctet());
    bool is_local(buffer.getOctet());
    buffer.getShortString(id);
    buffer.getShortString(excludes);
    bool dynamic(buffer.getOctet());
    uint16_t sync = buffer.getShort();

    return links.declare(host, port, durable, src, dest, key,
                         is_queue, is_local, id, excludes, dynamic, sync).first;
}

void Bridge::encode(Buffer& buffer) const
{
    buffer.putShortString(string("bridge"));
    buffer.putShortString(link->getHost());
    buffer.putShort(link->getPort());
    buffer.putOctet(args.i_durable ? 1 : 0);
    buffer.putShortString(args.i_src);
    buffer.putShortString(args.i_dest);
    buffer.putShortString(args.i_key);
    buffer.putOctet(args.i_srcIsQueue ? 1 : 0);
    buffer.putOctet(args.i_srcIsLocal ? 1 : 0);
    buffer.putShortString(args.i_tag);
    buffer.putShortString(args.i_excludes);
    buffer.putOctet(args.i_dynamic ? 1 : 0);
    buffer.putShort(args.i_sync);
}

uint32_t Bridge::encodedSize() const
{
    return link->getHost().size() + 1 // short-string (host)
        + 7                // short-string ("bridge")
        + 2                // port
        + 1                // durable
        + args.i_src.size()  + 1
        + args.i_dest.size() + 1
        + args.i_key.size()  + 1
        + 1                // srcIsQueue
        + 1                // srcIsLocal
        + args.i_tag.size() + 1
        + args.i_excludes.size() + 1
        + 1               // dynamic
        + 2;              // sync
}

management::ManagementObject* Bridge::GetManagementObject (void) const
{
    return (management::ManagementObject*) mgmtObject;
}

management::Manageable::status_t Bridge::ManagementMethod(uint32_t methodId,
                                                          management::Args& /*args*/,
                                                          string&)
{
    if (methodId == _qmf::Bridge::METHOD_CLOSE) {
        //notify that we are closed
        destroy();
        return management::Manageable::STATUS_OK;
    } else {
        return management::Manageable::STATUS_UNKNOWN_METHOD;
    }
}

void Bridge::propagateBinding(const string& key, const string& tagList,
                              const string& op,  const string& origin,
                              qpid::framing::FieldTable* extra_args)
{
    const string& localTag = link->getBroker()->getFederationTag();
    const string& peerTag  = connState->getFederationPeerTag();

    if (tagList.find(peerTag) == tagList.npos) {
         FieldTable bindArgs;
         if (extra_args) {
             for (qpid::framing::FieldTable::ValueMap::iterator i=extra_args->begin(); i != extra_args->end(); ++i) {
                 bindArgs.insert((*i));
             }
         }
         string newTagList(tagList + string(tagList.empty() ? "" : ",") + localTag);

         bindArgs.setString(qpidFedOp, op);
         bindArgs.setString(qpidFedTags, newTagList);
         if (origin.empty())
             bindArgs.setString(qpidFedOrigin, localTag);
         else
             bindArgs.setString(qpidFedOrigin, origin);

         conn->requestIOProcessing(boost::bind(&Bridge::ioThreadPropagateBinding, this,
                                               queueName, args.i_src, key, bindArgs));
    }
}

void Bridge::sendReorigin()
{
    FieldTable bindArgs;

    bindArgs.setString(qpidFedOp, fedOpReorigin);
    bindArgs.setString(qpidFedTags, link->getBroker()->getFederationTag());

    conn->requestIOProcessing(boost::bind(&Bridge::ioThreadPropagateBinding, this,
                                          queueName, args.i_src, args.i_key, bindArgs));
}
bool Bridge::resetProxy()
{
    SessionHandler& sessionHandler = conn->getChannel(id);
    if (!sessionHandler.getSession()) peer.reset();
    else peer.reset(new framing::AMQP_ServerProxy(sessionHandler.out));
    return peer.get();
}

void Bridge::ioThreadPropagateBinding(const string& queue, const string& exchange, const string& key, FieldTable args)
{
    if (resetProxy()) {
        peer->getExchange().bind(queue, exchange, key, args);
    } else {
        QPID_LOG(error, "Cannot propagate binding for dynamic bridge as session has been detached, deleting dynamic bridge");
        destroy();
    }
}

bool Bridge::containsLocalTag(const string& tagList) const
{
    const string& localTag = link->getBroker()->getFederationTag();
    return (tagList.find(localTag) != tagList.npos);
}

const string& Bridge::getLocalTag() const
{
    return link->getBroker()->getFederationTag();
}

}}
