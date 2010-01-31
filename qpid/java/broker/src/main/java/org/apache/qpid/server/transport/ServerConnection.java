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
package org.apache.qpid.server.transport;

import org.apache.qpid.server.configuration.ConnectionConfig;
import org.apache.qpid.server.virtualhost.VirtualHost;
import org.apache.qpid.transport.Connection;
import org.apache.qpid.transport.Method;

public class ServerConnection extends Connection
{
    private ConnectionConfig _config;
    private Runnable _onOpenTask;

    public ServerConnection()
    {
    }

    @Override
    protected void invoke(Method method)
    {
        super.invoke(method);
    }

    @Override
    protected void setState(State state)
    {
        super.setState(state);
        if(state == State.OPEN && _onOpenTask != null)
        {
            _onOpenTask.run();
        }
    }

    @Override
    public ServerConnectionDelegate getConnectionDelegate()
    {
        return (ServerConnectionDelegate) super.getConnectionDelegate();
    }

    public void setConnectionDelegate(ServerConnectionDelegate delegate)
    {
        super.setConnectionDelegate(delegate);
    }

    private VirtualHost _virtualHost;


    public VirtualHost getVirtualHost()
    {
        return _virtualHost;
    }

    public void setVirtualHost(VirtualHost virtualHost)
    {
        _virtualHost = virtualHost;
    }

    public void setConnectionConfig(final ConnectionConfig config)
    {
        _config = config;
    }

    public ConnectionConfig getConfig()
    {
        return _config;
    }

    public void onOpen(final Runnable task)
    {
        _onOpenTask = task;
    }
}
