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
#include "UpdateDataExchange.h"
#include "qpid/amqp_0_10/Codecs.h"
#include "qpid/broker/Deliverable.h"
#include "qpid/broker/Message.h"
#include "qpid/log/Statement.h"
#include "qpid/management/ManagementAgent.h"
#include "qpid/types/Variant.h"

namespace qpid {
namespace cluster {

const std::string UpdateDataExchange::EXCHANGE_NAME("qpid.cluster-update-data");
const std::string UpdateDataExchange::EXCHANGE_TYPE("qpid.cluster-update-data");
const std::string UpdateDataExchange::MANAGEMENT_AGENTS_KEY("management-agents");
const std::string UpdateDataExchange::MANAGEMENT_SCHEMAS_KEY("management-schemas");
const std::string UpdateDataExchange::MANAGEMENT_DELETED_OBJECTS_KEY("management-deleted-objects");

UpdateDataExchange::UpdateDataExchange(management::Manageable* parent,
                                       management::ManagementAgent* agent_) :
    Exchange(EXCHANGE_NAME, parent),
    agent(agent_)
{}

void UpdateDataExchange::route(broker::Deliverable& msg, const std::string& routingKey,
                               const qpid::framing::FieldTable* )
{
    std::string data = msg.getMessage().getFrames().getContent();
    if (routingKey == MANAGEMENT_AGENTS_KEY)
        managementAgents(data);
    else if (routingKey == MANAGEMENT_SCHEMAS_KEY)
        managementSchemas(data);
    else if (routingKey == MANAGEMENT_DELETED_OBJECTS_KEY)
        managementDeletedObjects(data);
    else
        throw Exception(
            QPID_MSG("Cluster update-data exchange received unknown routing-key: "
                     << routingKey));
}

void UpdateDataExchange::managementAgents(const std::string& data) {
    if (!agent)
        throw Exception(
            QPID_MSG("Received management agent update but management is disabled."));
    framing::Buffer buf(const_cast<char*>(data.data()), data.size());
    agent->importAgents(buf);
    QPID_LOG(debug, " Updated management agents.");
}

void UpdateDataExchange::managementSchemas(const std::string& data) {
    if (!agent)
        throw Exception(
            QPID_MSG("Received management schema update but management is disabled."));
    framing::Buffer buf(const_cast<char*>(data.data()), data.size());
    agent->importSchemas(buf);
    QPID_LOG(debug, " Updated management schemas");
}

void UpdateDataExchange::managementDeletedObjects(const std::string& data) {
    using amqp_0_10::ListCodec;
    using types::Variant;
    if (!agent)
        throw Exception(
            QPID_MSG("Management agent update but management not enabled."));
    Variant::List encoded;
    ListCodec::decode(data, encoded);
    management::ManagementAgent::DeletedObjectList objects;
    for (Variant::List::iterator i = encoded.begin(); i != encoded.end(); ++i) {
        objects.push_back(management::ManagementAgent::DeletedObject::shared_ptr(
                              new management::ManagementAgent::DeletedObject(*i)));
    }
    agent->importDeletedObjects(objects);
    QPID_LOG(debug, " Updated management deleted objects.");
}


}} // namespace qpid::cluster
