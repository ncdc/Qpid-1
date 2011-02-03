#ifndef QMF_AGENT_SESSION_H
#define QMF_AGENT_SESSION_H
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

#include <qmf/ImportExport.h>
#include "qmf/Handle.h"
#include "qpid/messaging/Duration.h"
#include "qpid/messaging/Connection.h"
#include "qpid/types/Variant.h"
#include <string>

namespace qmf {

#ifndef SWIG
    template <class> class PrivateImplRef;
#endif

    class AgentSessionImpl;
    class AgentEvent;
    class Schema;
    class Data;
    class DataAddr;

    class AgentSession : public qmf::Handle<AgentSessionImpl> {
    public:
        QMF_EXTERN AgentSession(AgentSessionImpl* impl = 0);
        QMF_EXTERN AgentSession(const AgentSession&);
        QMF_EXTERN AgentSession& operator=(const AgentSession&);
        QMF_EXTERN ~AgentSession();

        /**
         * AgentSession
         *   A session that runs over an AMQP connection for QMF agent operation.
         *
         * @param connection - An opened qpid::messaging::Connection
         * @param options - An optional string containing options
         *
         * The options string is of the form "{key:value,key:value}".  The following keys are supported:
         *
         *    interval:N                 - Heartbeat interval in seconds [default: 60]
         *    external:{True,False}      - Use external data storage (queries and subscriptions are pass-through) [default: False]
         *    allow-queries:{True,False} - If True:  automatically allow all queries [default]
         *                                 If False: generate an AUTH_QUERY event to allow per-query authorization
         *    allow-methods:{True,False} - If True:  automatically allow all methods [default]
         *                                 If False: generate an AUTH_METHOD event to allow per-method authorization
         *    max-subscriptions:N        - Maximum number of concurrent subscription queries permitted [default: 64]
         *    min-sub-interval:N         - Minimum publish interval (in milliseconds) permitted for a subscription [default: 3000]
         *    sub-lifetime:N             - Lifetime (in seconds with no keepalive) for a subscription [default: 300]
         *    public-events:{True,False} - If True:  QMF events are sent to the topic exchange [default]
         *                                 If False: QMF events are only sent to authorized subscribers
         *    listen-on-direct:{True,False} - If True:  Listen on legacy direct-exchange address for backward compatibility [default]
         *                                    If False: Listen only on the routable direct address
         *    strict-security:{True,False}  - If True:  Cooperate with the broker to enforce string access control to the network
         *                                  - If False: Operate more flexibly with regard to use of messaging facilities [default]
         */
        QMF_EXTERN AgentSession(qpid::messaging::Connection&, const std::string& options="");

        /**
         * setDomain - Change the QMF domain that this agent will operate in.  If this is not called,
         * the domain will be "default".  Agents in a domain can be seen only by consoles in the same domain.
         * This must be called prior to opening the agent session.
         */
        QMF_EXTERN void setDomain(const std::string&);

        /**
         * Set identifying attributes of this agent.
         * setVendor   - Set the vendor string
         * setProduct  - Set the product name string
         * setInstance - Set the unique instance name (if not set, a UUID will be assigned)
         * These must be called prior to opening the agent session.
         */
        QMF_EXTERN void setVendor(const std::string&);
        QMF_EXTERN void setProduct(const std::string&);
        QMF_EXTERN void setInstance(const std::string&);

        /**
         * setAttribute - Set an arbitrary attribute for this agent.  The attributes are not used
         * to uniquely identify the agent but can be used as a search criteria when looking for agents.
         * This must be called prior to opening the agent session.
         */
        QMF_EXTERN void setAttribute(const std::string&, const qpid::types::Variant&);

        /**
         * Get the identifying name of the agent.
         */
        QMF_EXTERN const std::string& getName() const;

        /**
         * Open the agent session.  After opening the session, the domain, identifying strings, and attributes cannot
         * be changed.
         */
        QMF_EXTERN void open();

        /**
         * Close the session.  Once closed, the session no longer communicates on the messaging network.
         */
        QMF_EXTERN void close();

        /**
         * Get the next event from the agent session.  Events represent actions that must be acted upon by the
         * agent application.  This method blocks for up to the timeout if there are no events to be handled.
         * This method will typically be the focus of the agent application's main execution loop.
         */
        QMF_EXTERN bool nextEvent(AgentEvent&, qpid::messaging::Duration timeout=qpid::messaging::Duration::FOREVER);

        /**
         * Register a schema to be exposed by this agent.
         */
        QMF_EXTERN void registerSchema(Schema&);

        /**
         * Add data to be managed internally by the agent.  If the option external:True is selected, this call
         * should not be used.
         *
         * @param data - The data object being managed by the agent.
         * @param name - A name unique to this object to be used to address the object.
         *               If left default, a unique name will be assigned by the agent.
         * @param persistent - Set this to true if the data object is to be considered persistent
         *                     across different sessions.  If persistent, it is the agent application's
         *                     responsibility to ensure the name is the same each time it is added.
         */
        QMF_EXTERN DataAddr addData(Data&, const std::string& name="", bool persistent=false);

        /**
         * Delete data from internal agent management.
         */
        QMF_EXTERN void delData(const DataAddr&);

        /**
         * The following methods are used to respond to events received in nextEvent.
         *
         * authAccept - Accept an authorization request.
         * authReject - Reject/forbid an authorization request.
         * raiseException - indicate failure of an operation (i.e. query or method call).
         * response - Provide data in response to a query (only for option:  external:True)
         * complete - Indicate that the response to a query is complete (external:True only)
         * methodSuccess - Indicate the successful completion of a method call.
         */
        QMF_EXTERN void authAccept(AgentEvent&);
        QMF_EXTERN void authReject(AgentEvent&, const std::string& diag="");
        QMF_EXTERN void raiseException(AgentEvent&, const std::string&);
        QMF_EXTERN void raiseException(AgentEvent&, const Data&);
        QMF_EXTERN void response(AgentEvent&, const Data&);
        QMF_EXTERN void complete(AgentEvent&);
        QMF_EXTERN void methodSuccess(AgentEvent&);

        /**
         * Raise an event to be sent into the QMF network.
         *
         * @param data - A data object that contains the event contents.
         * @param severity - Explicit severity (from qmf/SchemaTypes.h).  If omitted, the severity is set to
         *        the default severity for the data's schema.  If the data has no schema, the severity defaults
         *        to SEV_NOTICE.
         */
        QMF_EXTERN void raiseEvent(const Data& data);
        QMF_EXTERN void raiseEvent(const Data& data, int severity);

#ifndef SWIG
    private:
        friend class qmf::PrivateImplRef<AgentSession>;
#endif
    };

}

#endif
