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

#include "qpid/broker/Link.h"
#include "qpid/broker/LinkRegistry.h"
#include "qpid/broker/Broker.h"
#include "qpid/broker/Connection.h"
#include "qpid/sys/Timer.h"
#include "qmf/org/apache/qpid/broker/EventBrokerLinkUp.h"
#include "qmf/org/apache/qpid/broker/EventBrokerLinkDown.h"
#include "boost/bind.hpp"
#include "qpid/log/Statement.h"
#include "qpid/framing/enum.h"
#include "qpid/framing/reply_exceptions.h"
#include "qpid/broker/AclModule.h"

namespace qpid {
namespace broker {

using framing::Buffer;
using framing::FieldTable;
using framing::UnauthorizedAccessException;
using framing::connection::CLOSE_CODE_CONNECTION_FORCED;
using management::ManagementAgent;
using management::ManagementObject;
using management::Manageable;
using management::Args;
using sys::Mutex;
using std::stringstream;
using std::string;
namespace _qmf = ::qmf::org::apache::qpid::broker;

struct LinkTimerTask : public sys::TimerTask {
    LinkTimerTask(Link& l, sys::Timer& t)
        : TimerTask(int64_t(l.getBroker()->getOptions().linkMaintenanceInterval*
                            sys::TIME_SEC),
                    "Link retry timer"),
          link(l), timer(t) {}

    void fire() {
        link.maintenanceVisit();
        setupNextFire();
        timer.add(this);
    }

    Link& link;
    sys::Timer& timer;
};

Link::Link(LinkRegistry*  _links,
           MessageStore*  _store,
           const string&        _host,
           uint16_t       _port,
           const string&        _transport,
           bool           _durable,
           const string&        _authMechanism,
           const string&        _username,
           const string&        _password,
           Broker*        _broker,
           Manageable*    parent)
    : links(_links), store(_store), host(_host), port(_port),
      transport(_transport),
      durable(_durable),
      authMechanism(_authMechanism), username(_username), password(_password),
      persistenceId(0), mgmtObject(0), broker(_broker), state(0),
      visitCount(0),
      currentInterval(1),
      closing(false),
      reconnectNext(0),         // Index of next address for reconnecting in url.
      channelCounter(1),
      connection(0),
      agent(0),
      timerTask(new LinkTimerTask(*this, broker->getTimer()))
{
    if (parent != 0 && broker != 0)
    {
        agent = broker->getManagementAgent();
        if (agent != 0)
        {
            mgmtObject = new _qmf::Link(agent, this, parent, _host, _port, _transport, _durable);
            agent->addObject(mgmtObject, 0, durable);
        }
    }
    if (links->isPassive()) {
        setStateLH(STATE_PASSIVE);
    } else {
        setStateLH(STATE_WAITING);
        startConnectionLH();
    }
    broker->getTimer().add(timerTask);
}

Link::~Link ()
{
    if (state == STATE_OPERATIONAL && connection != 0)
        connection->close(CLOSE_CODE_CONNECTION_FORCED, "closed by management");

    if (mgmtObject != 0)
        mgmtObject->resourceDestroy ();
}

void Link::setStateLH (int newState)
{
    if (newState == state)
        return;

    state = newState;

    if (hideManagement())
        return;

    switch (state)
    {
    case STATE_WAITING     : mgmtObject->set_state("Waiting");     break;
    case STATE_CONNECTING  : mgmtObject->set_state("Connecting");  break;
    case STATE_OPERATIONAL : mgmtObject->set_state("Operational"); break;
    case STATE_FAILED      : mgmtObject->set_state("Failed");      break;
    case STATE_CLOSED      : mgmtObject->set_state("Closed");      break;
    case STATE_PASSIVE     : mgmtObject->set_state("Passive");      break;
    }
}

void Link::startConnectionLH ()
{
    assert(state == STATE_WAITING);
    try {
        // Set the state before calling connect.  It is possible that connect
        // will fail synchronously and call Link::closed before returning.
        setStateLH(STATE_CONNECTING);
        broker->connect (host, boost::lexical_cast<std::string>(port), transport,
                         boost::bind (&Link::closed, this, _1, _2));
        QPID_LOG (debug, "Inter-broker link connecting to " << host << ":" << port);
    } catch(const std::exception& e) {
        QPID_LOG(error, "Link connection to " << host << ":" << port << " failed: "
                 << e.what());
        setStateLH(STATE_WAITING);
        if (!hideManagement())
            mgmtObject->set_lastError (e.what());
    }
}

void Link::established(Connection* c)
{
    if (state == STATE_PASSIVE) return;
    stringstream addr;
    addr << host << ":" << port;
    QPID_LOG (info, "Inter-broker link established to " << addr.str());

    if (!hideManagement() && agent)
        agent->raiseEvent(_qmf::EventBrokerLinkUp(addr.str()));

    Mutex::ScopedLock mutex(lock);
    setStateLH(STATE_OPERATIONAL);
    currentInterval = 1;
    visitCount      = 0;
    connection = c;
    if (closing)
        destroy();
    else // Process any IO tasks bridges added before established.
        connection->requestIOProcessing (boost::bind(&Link::ioThreadProcessing, this));
}


void Link::setUrl(const Url& u) {
    Mutex::ScopedLock mutex(lock);
    url = u;
    reconnectNext = 0;
}

void Link::opened() {
    Mutex::ScopedLock mutex(lock);
    if (!connection) return;
    // Get default URL from known-hosts if not already set
    if (url.empty()) {
        const std::vector<Url>& known = connection->getKnownHosts();
        // Flatten vector of URLs into a single URL listing all addresses.
        url.clear();
        for(size_t i = 0; i < known.size(); ++i)
            url.insert(url.end(), known[i].begin(), known[i].end());
        reconnectNext = 0;
        QPID_LOG(debug, "Known hosts for peer of inter-broker link: " << url);
    }
}

void Link::closed(int, std::string text)
{
    Mutex::ScopedLock mutex(lock);
    QPID_LOG (info, "Inter-broker link disconnected from " << host << ":" << port << " " << text);

    connection = 0;
    if (state == STATE_OPERATIONAL) {
        stringstream addr;
        addr << host << ":" << port;
        if (!hideManagement() && agent)
            agent->raiseEvent(_qmf::EventBrokerLinkDown(addr.str()));
    }

    for (Bridges::iterator i = active.begin(); i != active.end(); i++) {
        (*i)->closed();
        created.push_back(*i);
    }
    active.clear();

    if (state != STATE_FAILED && state != STATE_PASSIVE)
    {
        setStateLH(STATE_WAITING);
        if (!hideManagement())
            mgmtObject->set_lastError (text);
    }

    if (closing)
        destroy();
}

// Called in connection IO thread.
void Link::destroy ()
{
    Bridges toDelete;
    {
        Mutex::ScopedLock mutex(lock);

        QPID_LOG (info, "Inter-broker link to " << host << ":" << port << " removed by management");
        if (connection)
            connection->close(CLOSE_CODE_CONNECTION_FORCED, "closed by management");
        connection = 0;
        setStateLH(STATE_CLOSED);

        // Move the bridges to be deleted into a local vector so there is no
        // corruption of the iterator caused by bridge deletion.
        for (Bridges::iterator i = active.begin(); i != active.end(); i++) {
            (*i)->closed();
            toDelete.push_back(*i);
        }
        active.clear();

        for (Bridges::iterator i = created.begin(); i != created.end(); i++)
            toDelete.push_back(*i);
        created.clear();

        timerTask->cancel();
    }
    // Now delete all bridges on this link (don't hold the lock for this).
    for (Bridges::iterator i = toDelete.begin(); i != toDelete.end(); i++)
        (*i)->destroy();
    toDelete.clear();
    links->destroy (host, port);
}

void Link::add(Bridge::shared_ptr bridge)
{
    Mutex::ScopedLock mutex(lock);
    created.push_back (bridge);
    if (connection)
        connection->requestIOProcessing (boost::bind(&Link::ioThreadProcessing, this));

}

void Link::cancel(Bridge::shared_ptr bridge)
{
    bool needIOProcessing = false;
    {
        Mutex::ScopedLock mutex(lock);

        for (Bridges::iterator i = created.begin(); i != created.end(); i++) {
            if ((*i).get() == bridge.get()) {
                created.erase(i);
                break;
            }
        }
        for (Bridges::iterator i = active.begin(); i != active.end(); i++) {
            if ((*i).get() == bridge.get()) {
                cancellations.push_back(bridge);
                bridge->closed();
                active.erase(i);
                break;
            }
        }
        needIOProcessing = !cancellations.empty();
    }
    if (needIOProcessing && connection)
        connection->requestIOProcessing (boost::bind(&Link::ioThreadProcessing, this));
}

void Link::ioThreadProcessing()
{
    Mutex::ScopedLock mutex(lock);

    if (state != STATE_OPERATIONAL)
        return;

    // check for bridge session errors and recover
    if (!active.empty()) {
        Bridges::iterator removed = std::remove_if(
            active.begin(), active.end(), !boost::bind(&Bridge::isSessionReady, _1));
        for (Bridges::iterator i = removed; i != active.end(); ++i) {
            Bridge::shared_ptr  bridge = *i;
            bridge->closed();
            bridge->cancel(*connection);
            created.push_back(bridge);
        }
        active.erase(removed, active.end());
    }

    //process any pending creates and/or cancellations (do
    //cancellations first in case any of the creates represent
    //recreation of cancelled subscriptions
    if (!cancellations.empty()) {
        for (Bridges::iterator i = cancellations.begin(); i != cancellations.end(); ++i) {
            (*i)->cancel(*connection);
        }
        cancellations.clear();
    }
    if (!created.empty()) {
        for (Bridges::iterator i = created.begin(); i != created.end(); ++i) {
            active.push_back(*i);
            (*i)->create(*connection);
        }
        created.clear();
    }
}

void Link::maintenanceVisit ()
{
    Mutex::ScopedLock mutex(lock);

    if (state == STATE_WAITING)
    {
        visitCount++;
        if (visitCount >= currentInterval)
        {
            visitCount = 0;
            //switch host and port to next in url list if possible
            if (!tryFailoverLH()) {
                currentInterval *= 2;
                if (currentInterval > MAX_INTERVAL)
                    currentInterval = MAX_INTERVAL;
                startConnectionLH();
            }
        }
    }
    else if (state == STATE_OPERATIONAL && (!active.empty() || !created.empty() || !cancellations.empty()) && connection != 0)
        connection->requestIOProcessing (boost::bind(&Link::ioThreadProcessing, this));
    }

void Link::reconnectLH(const Address& a)
{
    host = a.host;
    port = a.port;
    transport = a.protocol;
    startConnectionLH();
    if (!hideManagement()) {
        stringstream errorString;
        errorString << "Failed over to " << a;
        mgmtObject->set_lastError(errorString.str());
    }
}

bool Link::tryFailoverLH() {
    assert(state == STATE_WAITING);
    if (reconnectNext >= url.size()) reconnectNext = 0;
    if (url.empty()) return false;
    Address next = url[reconnectNext++];
    if (next.host != host || next.port != port || next.protocol != transport) {
        links->changeAddress(Address(transport, host, port), next);
        QPID_LOG(debug, "Inter-broker link failing over to " << next.host << ":" << next.port);
        reconnectLH(next);
        return true;
    }
    return false;
}

// Management updates for a linke are inconsistent in a cluster, so they are
// suppressed.
bool Link::hideManagement() const {
    return !mgmtObject || ( broker && broker->isInCluster());
}

uint Link::nextChannel()
{
    Mutex::ScopedLock mutex(lock);

    return channelCounter++;
}

void Link::notifyConnectionForced(const string text)
{
    Mutex::ScopedLock mutex(lock);
    setStateLH(STATE_FAILED);
    if (!hideManagement())
        mgmtObject->set_lastError(text);
}

void Link::setPersistenceId(uint64_t id) const
{
    persistenceId = id;
}

const string& Link::getName() const
{
    return host;
}

Link::shared_ptr Link::decode(LinkRegistry& links, Buffer& buffer)
{
    string   host;
    uint16_t port;
    string   transport;
    string   authMechanism;
    string   username;
    string   password;

    buffer.getShortString(host);
    port = buffer.getShort();
    buffer.getShortString(transport);
    bool durable(buffer.getOctet());
    buffer.getShortString(authMechanism);
    buffer.getShortString(username);
    buffer.getShortString(password);

    return links.declare(host, port, transport, durable, authMechanism, username, password).first;
}

void Link::encode(Buffer& buffer) const
{
    buffer.putShortString(string("link"));
    buffer.putShortString(host);
    buffer.putShort(port);
    buffer.putShortString(transport);
    buffer.putOctet(durable ? 1 : 0);
    buffer.putShortString(authMechanism);
    buffer.putShortString(username);
    buffer.putShortString(password);
}

uint32_t Link::encodedSize() const
{
    return host.size() + 1 // short-string (host)
        + 5                // short-string ("link")
        + 2                // port
        + transport.size() + 1 // short-string(transport)
        + 1                // durable
        + authMechanism.size() + 1
        + username.size() + 1
        + password.size() + 1;
}

ManagementObject* Link::GetManagementObject (void) const
{
    return (ManagementObject*) mgmtObject;
}

void Link::close() {
    Mutex::ScopedLock mutex(lock);
    if (!closing) {
        closing = true;
        if (state != STATE_CONNECTING && connection) {
            //connection can only be closed on the connections own IO processing thread
            connection->requestIOProcessing(boost::bind(&Link::destroy, this));
        }
    }
}


Manageable::status_t Link::ManagementMethod (uint32_t op, Args& args, string& text)
{
    switch (op)
    {
    case _qmf::Link::METHOD_CLOSE :
        close();
        return Manageable::STATUS_OK;

    case _qmf::Link::METHOD_BRIDGE :
        _qmf::ArgsLinkBridge& iargs = (_qmf::ArgsLinkBridge&) args;
        QPID_LOG(debug, "Link::bridge() request received");

        // Durable bridges are only valid on durable links
        if (iargs.i_durable && !durable) {
            text = "Can't create a durable route on a non-durable link";
            return Manageable::STATUS_USER;
        }

        if (iargs.i_dynamic) {
            Exchange::shared_ptr exchange = getBroker()->getExchanges().get(iargs.i_src);
            if (exchange.get() == 0) {
                text = "Exchange not found";
                return Manageable::STATUS_USER;
            }
            if (!exchange->supportsDynamicBinding()) {
                text = "Exchange type does not support dynamic routing";
                return Manageable::STATUS_USER;
            }
        }

        std::pair<Bridge::shared_ptr, bool> result =
            links->declare (host, port, iargs.i_durable, iargs.i_src,
                            iargs.i_dest, iargs.i_key, iargs.i_srcIsQueue,
                            iargs.i_srcIsLocal, iargs.i_tag, iargs.i_excludes,
                            iargs.i_dynamic, iargs.i_sync);

        if (result.second && iargs.i_durable)
            store->create(*result.first);

        return Manageable::STATUS_OK;
    }

    return Manageable::STATUS_UNKNOWN_METHOD;
}

void Link::setPassive(bool passive)
{
    Mutex::ScopedLock mutex(lock);
    if (passive) {
        setStateLH(STATE_PASSIVE);
    } else {
        if (state == STATE_PASSIVE) {
            setStateLH(STATE_WAITING);
        } else {
            QPID_LOG(warning, "Ignoring attempt to activate non-passive link "
                     << host << ":" << port);
        }
    }
}

}} // namespace qpid::broker
