package org.apache.qpid.server.handler;
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

import org.apache.log4j.Logger;
import org.apache.qpid.AMQException;
import org.apache.qpid.framing.AMQMethodBody;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.framing.MethodRegistry;
import org.apache.qpid.framing.QueueUnbindBody;
import org.apache.qpid.framing.amqp_0_9.MethodRegistry_0_9;
import org.apache.qpid.framing.amqp_0_91.MethodRegistry_0_91;
import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.AMQChannel;
import org.apache.qpid.server.exchange.Exchange;
import org.apache.qpid.server.exchange.ExchangeRegistry;
import org.apache.qpid.server.protocol.AMQProtocolSession;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.queue.QueueRegistry;
import org.apache.qpid.server.state.AMQStateManager;
import org.apache.qpid.server.state.StateAwareMethodListener;
import org.apache.qpid.server.virtualhost.VirtualHost;

public class QueueUnbindHandler implements StateAwareMethodListener<QueueUnbindBody>
{
    private static final Logger _log = Logger.getLogger(QueueUnbindHandler.class);

    private static final QueueUnbindHandler _instance = new QueueUnbindHandler();

    public static QueueUnbindHandler getInstance()
    {
        return _instance;
    }

    private QueueUnbindHandler()
    {
    }

    public void methodReceived(AMQStateManager stateManager, QueueUnbindBody body, int channelId) throws AMQException
    {
        AMQProtocolSession session = stateManager.getProtocolSession();
        VirtualHost virtualHost = session.getVirtualHost();
        ExchangeRegistry exchangeRegistry = virtualHost.getExchangeRegistry();
        QueueRegistry queueRegistry = virtualHost.getQueueRegistry();


        final AMQQueue queue;
        final AMQShortString routingKey;

        if (body.getQueue() == null)
        {
            AMQChannel channel = session.getChannel(channelId);

            if (channel == null)
            {
                throw body.getChannelNotFoundException(channelId);
            }

            queue = channel.getDefaultQueue();

            if (queue == null)
            {
                throw body.getChannelException(AMQConstant.NOT_FOUND, "No default queue defined on channel and queue was null");
            }

            routingKey = body.getRoutingKey() == null ? null : body.getRoutingKey().intern();

        }
        else
        {
            queue = queueRegistry.getQueue(body.getQueue());
            routingKey = body.getRoutingKey() == null ? null : body.getRoutingKey().intern();
        }

        if (queue == null)
        {
            throw body.getChannelException(AMQConstant.NOT_FOUND, "Queue " + body.getQueue() + " does not exist.");
        }
        final Exchange exch = exchangeRegistry.getExchange(body.getExchange());
        if (exch == null)
        {
            throw body.getChannelException(AMQConstant.NOT_FOUND, "Exchange " + body.getExchange() + " does not exist.");
        }

        if(virtualHost.getBindingFactory().getBinding(String.valueOf(routingKey), queue, exch, FieldTable.convertToMap(body.getArguments())) == null)
        {
            throw body.getChannelException(AMQConstant.NOT_FOUND,"No such binding");
        }
        else
        {
            virtualHost.getBindingFactory().removeBinding(String.valueOf(routingKey), queue, exch, FieldTable.convertToMap(body.getArguments()));
        }

        
        if (_log.isInfoEnabled())
        {
            _log.info("Binding queue " + queue + " to exchange " + exch + " with routing key " + routingKey);
        }

        final MethodRegistry registry = session.getMethodRegistry();
        final AMQMethodBody responseBody;
        if (registry instanceof MethodRegistry_0_9)
        {
            responseBody = ((MethodRegistry_0_9)registry).createQueueUnbindOkBody();
        }
        else if (registry instanceof MethodRegistry_0_91)
        {
            responseBody = ((MethodRegistry_0_91)registry).createQueueUnbindOkBody();
        }
        else
        {
            // 0-8 does not support QueueUnbind
            throw new AMQException(AMQConstant.COMMAND_INVALID, "QueueUnbind not present in AMQP version: " + session.getProtocolVersion(), null);
        }
        session.writeFrame(responseBody.generateFrame(channelId));
    }
}
