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
#include "InitialStatusMap.h"
#include "StoreStatus.h"
#include "qpid/log/Statement.h"
#include <algorithm>
#include <vector>
#include <boost/bind.hpp>

namespace qpid {
namespace cluster {

using namespace std;
using namespace boost;
using namespace framing::cluster;
using namespace framing;

InitialStatusMap::InitialStatusMap(const MemberId& self_, size_t size_)
    : self(self_), completed(), resendNeeded(), size(size_)
{}

void InitialStatusMap::configChange(const MemberSet& members) {
    resendNeeded = false;
    bool wasComplete = isComplete();
    if (firstConfig.empty()) firstConfig = members;
    MemberSet::const_iterator i = members.begin();
    Map::iterator j = map.begin();
    while (i != members.end() || j != map.end()) {
        if (i == members.end()) { // j not in members, member left
            firstConfig.erase(j->first);
            Map::iterator k = j++;
            map.erase(k);
        }
        else if (j == map.end()) { // i not in map, member joined
            resendNeeded = true;
            map[*i] = optional<Status>();
            ++i;
        }
        else if (*i < j->first) { // i not in map, member joined
            resendNeeded = true;
            map[*i] = optional<Status>();
            ++i;
        }
        else if (*i > j->first) { // j not in members, member left
            firstConfig.erase(j->first);
            Map::iterator k = j++;
            map.erase(k);
        }
        else {
            i++; j++;
        }
    }
    if (resendNeeded) {         // Clear all status
        for (Map::iterator i = map.begin(); i != map.end(); ++i)
            i->second = optional<Status>();
    }
    completed = isComplete() && !wasComplete; // Set completed on the transition.
}

void InitialStatusMap::received(const MemberId& m, const Status& s){
    bool wasComplete = isComplete();
    map[m] = s;
    completed = isComplete() && !wasComplete; // Set completed on the transition.
}

bool InitialStatusMap::notInitialized(const Map::value_type& v) {
    return !v.second;
}

bool InitialStatusMap::isComplete() const {
    return !map.empty() && find_if(map.begin(), map.end(), &notInitialized) == map.end();
}

bool InitialStatusMap::transitionToComplete() {
    return completed;
}

bool InitialStatusMap::isResendNeeded() {
    bool ret = resendNeeded;
    resendNeeded = false;
    return ret;
}

bool InitialStatusMap::isActiveEntry(const Map::value_type& v) {
    return v.second && v.second->getActive();
}

bool InitialStatusMap::hasStore(const Map::value_type& v) {
    return v.second &&
        (v.second->getStoreState() == STORE_STATE_CLEAN_STORE ||
         v.second->getStoreState() == STORE_STATE_DIRTY_STORE);
}

bool InitialStatusMap::isActive() {
    assert(isComplete());
    return (find_if(map.begin(), map.end(), &isActiveEntry) != map.end());
}

bool InitialStatusMap::isUpdateNeeded() {
    assert(isComplete());
    // We need an update if there are any active members.
    if (isActive()) return true;

    // Otherwise it depends on store status, get my own status:
    Map::iterator me = map.find(self);
    assert(me != map.end());
    assert(me->second);
    switch (me->second->getStoreState()) {
      case STORE_STATE_NO_STORE:
      case STORE_STATE_EMPTY_STORE:
        // If anybody has a store then we need an update.
        return find_if(map.begin(), map.end(), &hasStore) != map.end();
      case STORE_STATE_DIRTY_STORE: return true; 
      case STORE_STATE_CLEAN_STORE: return false; // Use our own store
    }
    return false;
}

MemberSet InitialStatusMap::getElders() const {
    assert(isComplete());
    MemberSet elders;
    for (MemberSet::const_iterator i = firstConfig.begin(); i != firstConfig.end(); ++i) {
        // *i is in my first config, so a potential elder.
        if (*i == self) continue; // Not my own elder
        Map::const_iterator j = map.find(*i);
        assert(j != map.end());
        assert(j->second);
        const Status& s = *j->second;
        // If I'm not in i's first config then i is older than me.
        // Otherwise we were born in the same configuration so use
        // member ID to break the tie.
        MemberSet iFirstConfig = decodeMemberSet(s.getFirstConfig());
        if (iFirstConfig.find(self) == iFirstConfig.end() || *i > self)
            elders.insert(*i);
    }
    return elders;
}

// Get cluster ID from an active member or the youngest newcomer.
Uuid InitialStatusMap::getClusterId() {
    assert(isComplete());
    assert(!map.empty());
    Map::iterator i = find_if(map.begin(), map.end(), &isActiveEntry);
    if (i != map.end())
        return i->second->getClusterId(); // An active member
    else
        return map.begin()->second->getClusterId(); // Youngest newcomer in node-id order
}

void checkId(Uuid& expect, const Uuid& actual, const string& msg) {
    if (!expect) expect = actual;
    assert(expect);
    if (expect != actual)
        throw Exception(msg);
}

void InitialStatusMap::checkConsistent() {
    assert(isComplete());
    int clean = 0;
    int dirty = 0;
    int empty = 0;
    int none = 0;
    int active = 0;
    Uuid clusterId;
    Uuid shutdownId;

    bool initialCluster = !isActive();
    for (Map::iterator i = map.begin(); i != map.end(); ++i) {
        assert(i->second);
        if (i->second->getActive()) ++active;
        switch (i->second->getStoreState()) {
          case STORE_STATE_NO_STORE: ++none; break;
          case STORE_STATE_EMPTY_STORE: ++empty; break;
          case STORE_STATE_DIRTY_STORE:
            ++dirty;
            checkId(clusterId, i->second->getClusterId(),
                    "Cluster-ID mismatch. Stores belong to different clusters.");
            break;
          case STORE_STATE_CLEAN_STORE:
            ++clean;
            checkId(clusterId, i->second->getClusterId(),
                    "Cluster-ID mismatch. Stores belong to different clusters.");
            // Only need shutdownId to match if we are in an initially forming cluster.
            if (initialCluster)
                checkId(shutdownId, i->second->getShutdownId(),
                        "Shutdown-ID mismatch. Stores were not shut down together");
            break;
        }
    }
    // Can't mix transient and persistent members.
    if (none && (clean+dirty+empty))
        throw Exception("Mixing transient and persistent brokers in a cluster");

    if (map.size() >= size) {
        // All initial members are present. If there are no active
        // members and there are dirty stores there must be at least
        // one clean store.
        if (!active && dirty && !clean)
            throw Exception("Cannot recover, no clean store.");
    }
}

std::string InitialStatusMap::getFirstConfigStr() const {
    assert(!firstConfig.empty());
    return encodeMemberSet(firstConfig);
}

}} // namespace qpid::cluster
