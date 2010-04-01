#ifndef QPID_CLUSTER_STORESTATE_H
#define QPID_CLUSTER_STORESTATE_H

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

#include "qpid/framing/Uuid.h"
#include "qpid/framing/SequenceNumber.h"
#include "qpid/framing/enum.h"
#include <iosfwd>

namespace qpid {
namespace cluster {

/**
 * State of the store for cluster purposes.
 */
class StoreStatus
{
  public:
    typedef framing::Uuid Uuid;
    typedef framing::cluster::StoreState StoreState;

    StoreStatus(const std::string& dir);

    framing::cluster::StoreState getState() const { return state; }

    const Uuid& getClusterId() const { return clusterId; }
    void setClusterId(const Uuid&);
    const Uuid& getShutdownId() const { return shutdownId; }

    void load();
    void dirty();               // Mark the store in use.
    void clean(const Uuid& shutdownId); // Mark the store clean.
    bool hasStore() const;

  private:
    void save();

    framing::cluster::StoreState state;
    Uuid clusterId, shutdownId;
    std::string dataDir;
};

const char* stateName(framing::cluster::StoreState);
std::ostream& operator<<(std::ostream&, framing::cluster::StoreState);
std::ostream& operator<<(std::ostream&, const StoreStatus&);
}} // namespace qpid::cluster

#endif  /*!QPID_CLUSTER_STORESTATE_H*/
