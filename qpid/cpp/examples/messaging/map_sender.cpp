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

#include <qpid/messaging/Connection.h>
#include <qpid/messaging/MapContent.h>
#include <qpid/messaging/Message.h>
#include <qpid/messaging/Sender.h>
#include <qpid/messaging/Session.h>

#include <cstdlib>
#include <iostream>

#include <sstream>

using namespace qpid::messaging;

using std::stringstream;
using std::string;

int main(int argc, char** argv) {
    const char* url = argc>1 ? argv[1] : "amqp:tcp:127.0.0.1:5672";

    try {
        Connection connection = Connection::open(url);
        Session session = connection.newSession();
        Sender sender = session.createSender("message_queue");

        Message message;
        MapContent content(message);
        content["id"] = 987654321;
        content["name"] = "Widget";
        content["price"] = 0.99;//bad use of floating point number, just an example!
        Variant::List colours;
        colours.push_back(Variant("red"));
        colours.push_back(Variant("green"));
        colours.push_back(Variant("white"));
        content["colours"] = colours;
        content["uuid"] = Uuid(true);
        content.encode();
	
        sender.send(message);
        session.sync();

        connection.close();
        return 0;
    } catch(const std::exception& error) {
        std::cout << error.what() << std::endl;
    }
    return 1;
}


