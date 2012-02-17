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
#include "qpid/broker/LinkRegistry.h"
#include "qpid/broker/Link.h"
#include "qpid/broker/Connection.h"
#include "qpid/log/Statement.h"
#include <iostream>
#include <boost/format.hpp>

using namespace qpid::broker;
using namespace qpid::sys;
using std::string;
using std::pair;
using std::stringstream;
using boost::intrusive_ptr;
using boost::format;
using boost::str;
namespace _qmf = qmf::org::apache::qpid::broker;

// TODO: This constructor is only used by the store unit tests -
// That probably indicates that LinkRegistry isn't correctly
// factored: The persistence element should be factored separately
LinkRegistry::LinkRegistry () :
    broker(0),
    parent(0), store(0), passive(false),
    realm("")
{
}

namespace {
struct ConnectionObserverImpl : public ConnectionObserver {
    LinkRegistry& links;
    ConnectionObserverImpl(LinkRegistry& l) : links(l) {}
    void connection(Connection& c) { links.notifyConnection(c.getMgmtId(), &c); }
    void opened(Connection& c) { links.notifyOpened(c.getMgmtId()); }
    void closed(Connection& c) { links.notifyClosed(c.getMgmtId()); }
    void forced(Connection& c, const string& text) { links.notifyConnectionForced(c.getMgmtId(), text); }
};
}

LinkRegistry::LinkRegistry (Broker* _broker) :
    broker(_broker),
    parent(0), store(0), passive(false),
    realm(broker->getOptions().realm)
{
    broker->getConnectionObservers().add(
        boost::shared_ptr<ConnectionObserver>(new ConnectionObserverImpl(*this)));
}

LinkRegistry::~LinkRegistry() {}


void LinkRegistry::changeAddress(const qpid::Address& oldAddress, const qpid::Address& newAddress)
{
    Mutex::ScopedLock   locker(lock);
    std::string oldKey = createKey(oldAddress);
    std::string newKey = createKey(newAddress);
    if (links.find(newKey) != links.end()) {
        QPID_LOG(error, "Attempted to update key from " << oldKey << " to " << newKey << " which is already in use");
    } else {
        LinkMap::iterator i = links.find(oldKey);
        if (i == links.end()) {
            QPID_LOG(error, "Attempted to update key from " << oldKey << " which does not exist, to " << newKey);
        } else {
            links[newKey] = i->second;
            links.erase(oldKey);
            QPID_LOG(info, "Updated link key from " << oldKey << " to " << newKey);
        }
    }
}

pair<Link::shared_ptr, bool> LinkRegistry::declare(const string&  host,
                                                   uint16_t port,
                                                   const string&  transport,
                                                   bool     durable,
                                                   const string&  authMechanism,
                                                   const string&  username,
                                                   const string&  password)

{
    Mutex::ScopedLock   locker(lock);
    string key = createKey(host, port);

    LinkMap::iterator i = links.find(key);
    if (i == links.end())
    {
        Link::shared_ptr link;

        link = Link::shared_ptr (new Link (this, store, host, port, transport, durable,
                                           authMechanism, username, password,
                                           broker, parent));
        links[key] = link;
        return std::pair<Link::shared_ptr, bool>(link, true);
    }
    return std::pair<Link::shared_ptr, bool>(i->second, false);
}

pair<Bridge::shared_ptr, bool> LinkRegistry::declare(const std::string& host,
                                                     uint16_t     port,
                                                     bool         durable,
                                                     const std::string& src,
                                                     const std::string& dest,
                                                     const std::string& key,
                                                     bool         isQueue,
                                                     bool         isLocal,
                                                     const std::string& tag,
                                                     const std::string& excludes,
                                                     bool         dynamic,
                                                     uint16_t     sync,
                                                     Bridge::InitializeCallback init
)
{
    Mutex::ScopedLock locker(lock);
    QPID_LOG(debug, "Bridge declared " << host << ": " << port << " from " << src << " to " << dest << " (" << key << ")");

    string linkKey = createKey(host, port);
    stringstream keystream;
    keystream << linkKey << "!" << src << "!" << dest << "!" << key;
    string bridgeKey = keystream.str();

    LinkMap::iterator l = links.find(linkKey);
    if (l == links.end())
        return pair<Bridge::shared_ptr, bool>(Bridge::shared_ptr(), false);

    BridgeMap::iterator b = bridges.find(bridgeKey);
    if (b == bridges.end())
    {
        _qmf::ArgsLinkBridge args;
        Bridge::shared_ptr bridge;

        args.i_durable    = durable;
        args.i_src        = src;
        args.i_dest       = dest;
        args.i_key        = key;
        args.i_srcIsQueue = isQueue;
        args.i_srcIsLocal = isLocal;
        args.i_tag        = tag;
        args.i_excludes   = excludes;
        args.i_dynamic    = dynamic;
        args.i_sync       = sync;

        bridge = Bridge::shared_ptr
            (new Bridge (l->second.get(), l->second->nextChannel(),
                         boost::bind(&LinkRegistry::destroy, this,
                                     host, port, src, dest, key),
                         args, init));
        bridges[bridgeKey] = bridge;
        l->second->add(bridge);
        return std::pair<Bridge::shared_ptr, bool>(bridge, true);
    }
    return std::pair<Bridge::shared_ptr, bool>(b->second, false);
}

void LinkRegistry::destroy(const string& host, const uint16_t port)
{
    Mutex::ScopedLock   locker(lock);
    string key = createKey(host, port);

    LinkMap::iterator i = links.find(key);
    if (i != links.end())
    {
        if (i->second->isDurable() && store)
            store->destroy(*(i->second));
        links.erase(i);
    }
}

void LinkRegistry::destroy(const std::string& host,
                           const uint16_t     port,
                           const std::string& src,
                           const std::string& dest,
                           const std::string& key)
{
    Mutex::ScopedLock locker(lock);
    string linkKey = createKey(host, port);
    stringstream keystream;
    keystream << linkKey << "!" << src << "!" << dest << "!" << key;
    string bridgeKey = keystream.str();

    LinkMap::iterator l = links.find(linkKey);
    if (l == links.end())
        return;

    BridgeMap::iterator b = bridges.find(bridgeKey);
    if (b == bridges.end())
        return;

    l->second->cancel(b->second);
    if (b->second->isDurable())
        store->destroy(*(b->second));
    bridges.erase(b);
}

void LinkRegistry::setStore (MessageStore* _store)
{
    store = _store;
}

MessageStore* LinkRegistry::getStore() const {
    return store;
}

Link::shared_ptr LinkRegistry::findLink(const std::string& keyOrMgmtId)
{
    // Convert keyOrMgmtId to a host:port key.
    //
    // TODO aconway 2011-02-01: centralize code that constructs/parses
    // connection management IDs. Currently sys:: protocol factories
    // and IO plugins construct the IDs and LinkRegistry parses them.
    size_t separator = keyOrMgmtId.find('-');
    if (separator == std::string::npos) separator = 0;
    std::string key =  keyOrMgmtId.substr(separator+1, std::string::npos);

    Mutex::ScopedLock locker(lock);
    LinkMap::iterator l = links.find(key);
    if (l != links.end()) return l->second;
    else return Link::shared_ptr();
}

void LinkRegistry::notifyConnection(const std::string& key, Connection* c)
{
    Link::shared_ptr link = findLink(key);
    if (link) {
        link->established(c);
        c->setUserId(str(format("%1%@%2%") % link->getUsername() % realm));
    }
}

void LinkRegistry::notifyOpened(const std::string& key)
{
    Link::shared_ptr link = findLink(key);
    if (link) link->opened();
}

void LinkRegistry::notifyClosed(const std::string& key)
{
    Link::shared_ptr link = findLink(key);
    if (link) {
        link->closed(0, "Closed by peer");
    }
}

void LinkRegistry::notifyConnectionForced(const std::string& key, const std::string& text)
{
    Link::shared_ptr link = findLink(key);
    if (link) {
        link->notifyConnectionForced(text);
    }
}

std::string LinkRegistry::getAuthMechanism(const std::string& key)
{
    Link::shared_ptr link = findLink(key);
    if (link)
        return link->getAuthMechanism();
    return string("ANONYMOUS");
}

std::string LinkRegistry::getAuthCredentials(const std::string& key)
{
    Link::shared_ptr link = findLink(key);
    if (!link)
        return string();

    string result;
    result += '\0';
    result += link->getUsername();
    result += '\0';
    result += link->getPassword();

    return result;
}

std::string LinkRegistry::getUsername(const std::string& key)
{
    Link::shared_ptr link = findLink(key);
    if (!link)
        return string();

    return link->getUsername();
}

std::string LinkRegistry::getHost(const std::string& key)
{
    Link::shared_ptr link = findLink(key);
    if (!link)
        return string();

    return link->getHost();
}

uint16_t LinkRegistry::getPort(const std::string& key)
{
    Link::shared_ptr link = findLink(key);
    if (!link)
        return 0;

    return link->getPort();
}

std::string LinkRegistry::getPassword(const std::string& key)
{
    Link::shared_ptr link = findLink(key);
    if (!link)
        return string();

    return link->getPassword();
}

std::string LinkRegistry::getAuthIdentity(const std::string& key)
{
    Link::shared_ptr link = findLink(key);
    if (!link)
        return string();

    return link->getUsername();
}


std::string LinkRegistry::createKey(const qpid::Address& a) {
    // TODO aconway 2010-05-11: key should also include protocol/transport to
    // be unique. Requires refactor of LinkRegistry interface.
    return createKey(a.host, a.port);
}

std::string LinkRegistry::createKey(const std::string& host,  uint16_t port) {
    // TODO aconway 2010-05-11: key should also include protocol/transport to
    // be unique. Requires refactor of LinkRegistry interface.
    stringstream keystream;
    keystream << host << ":" << port;
    return keystream.str();
}

void LinkRegistry::setPassive(bool p)
{
    Mutex::ScopedLock locker(lock);
    passive = p;
    if (passive) { QPID_LOG(info, "Passivating links"); }
    else { QPID_LOG(info, "Activating links"); }
    for (LinkMap::iterator i = links.begin(); i != links.end(); i++) {
        i->second->setPassive(passive);
    }
}

void LinkRegistry::eachLink(boost::function<void(boost::shared_ptr<Link>)> f) {
    for (LinkMap::iterator i = links.begin(); i != links.end(); ++i) f(i->second);
}

void LinkRegistry::eachBridge(boost::function<void(boost::shared_ptr<Bridge>)> f) {
    for (BridgeMap::iterator i = bridges.begin(); i != bridges.end(); ++i) f(i->second);
}

