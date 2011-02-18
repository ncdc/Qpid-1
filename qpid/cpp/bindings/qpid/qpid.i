/*
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
 */

%{

#include <qpid/messaging/exceptions.h>
#include <qpid/messaging/Address.h>
#include <qpid/messaging/Connection.h>
#include <qpid/messaging/Session.h>
#include <qpid/messaging/Receiver.h>
#include <qpid/messaging/Sender.h>
#include <qpid/messaging/Message.h>
#include <qpid/messaging/Duration.h>
#include <qpid/messaging/FailoverUpdates.h>

//
// Wrapper functions for map-decode and list-decode.  This allows us to avoid
// the complexity of output parameter mapping.
//
qpid::types::Variant::Map& decodeMap(const qpid::messaging::Message& msg) {
    static qpid::types::Variant::Map map;
    map.clear();
    qpid::messaging::decode(msg, map);
    return map;
}

qpid::types::Variant::List& decodeList(const qpid::messaging::Message& msg) {
    static qpid::types::Variant::List list;
    list.clear();
    qpid::messaging::decode(msg, list);
    return list;
}

%}

%include <qpid/messaging/ImportExport.h>
%include <qpid/messaging/Address.h>
%include <qpid/messaging/Duration.h>
%include <qpid/messaging/Message.h>
%include <qpid/messaging/Receiver.h>
%include <qpid/messaging/Sender.h>
%include <qpid/messaging/Session.h>
%include <qpid/messaging/Connection.h>
%include <qpid/messaging/FailoverUpdates.h>

qpid::types::Variant::Map& decodeMap(const qpid::messaging::Message&);
qpid::types::Variant::List& decodeList(const qpid::messaging::Message&);


%{

%};

