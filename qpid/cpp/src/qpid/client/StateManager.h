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
#ifndef _StateManager_
#define _StateManager_

#include <set>
#include "qpid/sys/Monitor.h"

namespace qpid {
namespace client {

///@internal
class StateManager
{
    int state;
    mutable sys::Monitor stateLock;
    
public:
    StateManager(int initial);
    void setState(int state);
    bool setState(int state, int expected);
    int getState() const ;
    void waitForStateChange(int current);
    void waitFor(std::set<int> states);
    void waitFor(int state);
    bool waitFor(std::set<int> states, qpid::sys::Duration);
    bool waitFor(int state, qpid::sys::Duration);
};

}}

#endif
