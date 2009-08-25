#ifndef QPID_MESSAGING_CONNECTION_H
#define QPID_MESSAGING_CONNECTION_H

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
#include <string>
#include "qpid/client/ClientImportExport.h"
#include "qpid/client/Handle.h"
#include "qpid/messaging/Variant.h"

namespace qpid {
namespace client {

template <class> class PrivateImplRef;

}

namespace messaging {

class ConnectionImpl;
class Session;

class Connection : public qpid::client::Handle<ConnectionImpl>
{
  public:
    static Connection open(const std::string& url, const Variant::Map& options = Variant::Map());

    QPID_CLIENT_EXTERN Connection(ConnectionImpl* impl = 0);
    QPID_CLIENT_EXTERN Connection(const Connection&);
    QPID_CLIENT_EXTERN ~Connection();
    QPID_CLIENT_EXTERN Connection& operator=(const Connection&);
    QPID_CLIENT_EXTERN void close();
    QPID_CLIENT_EXTERN Session newSession();
  private:
  friend class qpid::client::PrivateImplRef<Connection>;

};

struct InvalidOptionString : public qpid::Exception 
{
    InvalidOptionString(const std::string& msg);
};

QPID_CLIENT_EXTERN void parseOptionString(const std::string&, Variant::Map&);
QPID_CLIENT_EXTERN Variant::Map parseOptionString(const std::string&);

}} // namespace qpid::messaging

#endif  /*!QPID_MESSAGING_CONNECTION_H*/
