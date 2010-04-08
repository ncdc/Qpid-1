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
#include <qpid/messaging/Message.h>
#include <qpid/messaging/Receiver.h>
#include <qpid/messaging/Session.h>

#include <cstdlib>
#include <iostream>

using namespace qpid::messaging;

int main(int argc, char** argv) {
    const std::string url = argc>1 ? argv[1] : "amqp:tcp:127.0.0.1:5672";
    const std::string pattern = argc>2 ? argv[2] : "#.#";

    Connection connection(url);
    try {
        connection.connect();
        Session session = connection.createSession();
        Receiver receiver = session.createReceiver("news_service; {filter:[control, " + pattern + "]}");
        while (true) {
            Message message = receiver.fetch();
            std::cout << "Message: " << message.getContent() << std::endl;
            if (message.getContent() == "That's all, folks!") {
                std::cout << "Closing receiver" << std::endl;
                receiver.close();
                break;
            }
        }

        connection.close();
        return 0;
    } catch(const std::exception& error) {
        std::cout << error.what() << std::endl;
        connection.close();
    }
    return 1;   
}


