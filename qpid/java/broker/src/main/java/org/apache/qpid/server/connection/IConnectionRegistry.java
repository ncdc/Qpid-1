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
package org.apache.qpid.server.connection;

import org.apache.qpid.AMQException;
import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.protocol.AMQConnectionModel;

import java.util.List;

public interface IConnectionRegistry
{
    public static final String BROKER_SHUTDOWN_REPLY_TEXT = "Broker is shutting down";
    public static final String VHOST_PASSIVATE_REPLY_TEXT = "Virtual host is being passivated";

    public void initialise();

    public void close() throws AMQException;

    public void close(String replyText) throws AMQException;

    public List<AMQConnectionModel> getConnections();

    public void registerConnection(AMQConnectionModel connnection);

    public void deregisterConnection(AMQConnectionModel connnection);

    void addRegistryChangeListener(RegistryChangeListener listener);

    interface RegistryChangeListener
    {
        void connectionRegistered(AMQConnectionModel connection);
        void connectionUnregistered(AMQConnectionModel connection);

    }
}
