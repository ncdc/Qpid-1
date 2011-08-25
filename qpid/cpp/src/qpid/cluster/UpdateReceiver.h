#ifndef QPID_CLUSTER_UPDATESTATE_H
#define QPID_CLUSTER_UPDATESTATE_H

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

#include "Numbering.h"
#include "qpid/broker/SemanticState.h"

namespace qpid {
namespace cluster {

/**
 * Cluster-wide state used when receiving an update.
 */
class UpdateReceiver {
  public:
    /** Numbering used to identify Queue listeners as consumers */ 
    typedef Numbering<boost::shared_ptr<broker::SemanticState::ConsumerImpl> > ConsumerNumbering;
    ConsumerNumbering consumerNumbering;

    /** Management-id for the next shadow connection */
    std::string nextShadowMgmtId;

    /** Relationship between DtxBuffers, identified by xid, index in DtxManager,
     * and sessions represented by their SemanticState.
     */
    typedef std::pair<std::string, uint32_t> DtxBufferRef;
    typedef std::map<DtxBufferRef, broker::SemanticState* > DtxBuffers;
    DtxBuffers dtxBuffers;
};
}} // namespace qpid::cluster

#endif  /*!QPID_CLUSTER_UPDATESTATE_H*/
