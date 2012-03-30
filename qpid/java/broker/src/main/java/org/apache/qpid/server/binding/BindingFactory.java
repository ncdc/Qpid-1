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
package org.apache.qpid.server.binding;

import org.apache.qpid.AMQException;
import org.apache.qpid.AMQInternalException;
import org.apache.qpid.AMQSecurityException;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.server.configuration.BindingConfig;
import org.apache.qpid.server.configuration.BindingConfigType;
import org.apache.qpid.server.configuration.ConfigStore;
import org.apache.qpid.server.configuration.ConfiguredObject;
import org.apache.qpid.server.exchange.Exchange;
import org.apache.qpid.server.logging.actors.CurrentActor;
import org.apache.qpid.server.logging.messages.BindingMessages;
import org.apache.qpid.server.logging.subjects.BindingLogSubject;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.virtualhost.VirtualHost;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BindingFactory
{
    private final VirtualHost _virtualHost;

    private final ConcurrentHashMap<BindingImpl, BindingImpl> _bindings = new ConcurrentHashMap<BindingImpl, BindingImpl>();

    public BindingFactory(final VirtualHost vhost)
    {
        _virtualHost = vhost;
    }

    private final class BindingImpl extends Binding implements AMQQueue.Task, Exchange.Task, BindingConfig
    {
        private final BindingLogSubject _logSubject;
        //TODO : persist creation time
        private long _createTime = System.currentTimeMillis();

        private BindingImpl(String bindingKey, final AMQQueue queue, final Exchange exchange, final Map<String, Object> arguments)
        {
            super(queue.getVirtualHost().getConfigStore().createId(), bindingKey, queue, exchange, arguments);
            _logSubject = new BindingLogSubject(bindingKey,exchange,queue);

        }


        public void doTask(final AMQQueue queue) throws AMQException
        {
            removeBinding(this);
        }

        public void onClose(final Exchange exchange) throws AMQSecurityException, AMQInternalException
        {
            removeBinding(this);
        }

        void logCreation()
        {
            CurrentActor.get().message(_logSubject, BindingMessages.CREATED(String.valueOf(getArguments()), getArguments() != null && !getArguments().isEmpty()));
        }

        void logDestruction()
        {
            CurrentActor.get().message(_logSubject, BindingMessages.DELETED());
        }

        public String getOrigin()
        {
            return (String) getArguments().get("qpid.fed.origin");
        }

        public long getCreateTime()
        {
            return _createTime;
        }

        public BindingConfigType getConfigType()
        {
            return BindingConfigType.getInstance();
        }

        public ConfiguredObject getParent()
        {
            return _virtualHost;
        }

        public boolean isDurable()
        {
            return getQueue().isDurable() && getExchange().isDurable();
        }

    }



    public boolean addBinding(String bindingKey, AMQQueue queue, Exchange exchange, Map<String, Object> arguments) throws AMQSecurityException, AMQInternalException 
    {
        return makeBinding(bindingKey, queue, exchange, arguments, false, false);
    }


    public boolean replaceBinding(final String bindingKey,
                               final AMQQueue queue,
                               final Exchange exchange,
                               final Map<String, Object> arguments) throws AMQSecurityException, AMQInternalException
    {
        return makeBinding(bindingKey, queue, exchange, arguments, false, true);
    }

    private boolean makeBinding(String bindingKey, AMQQueue queue, Exchange exchange, Map<String, Object> arguments, boolean restore, boolean force) throws AMQSecurityException, AMQInternalException
    {
        assert queue != null;
        final Exchange defaultExchange = _virtualHost.getExchangeRegistry().getDefaultExchange();

        if (bindingKey == null)
        {
            bindingKey = "";
        }
        if (exchange == null)
        {
            exchange = defaultExchange;
        }
        if (arguments == null)
        {
            arguments = Collections.emptyMap();
        }

        if (exchange == null)
        {
            throw new IllegalArgumentException("exchange cannot be null");
        }

        // The default exchange bindings must reflect the existence of queues, allow
        // all operations on it to succeed. It is up to the broker to prevent illegal
        // attempts at binding to this exchange, not the ACLs.
        if(exchange != defaultExchange)
        {
            //Perform ACLs
            if (!_virtualHost.getSecurityManager().authoriseBind(exchange, queue, new AMQShortString(bindingKey)))
            {
                throw new AMQSecurityException("Permission denied: binding " + bindingKey);
            }
        }

        
        BindingImpl b = new BindingImpl(bindingKey,queue,exchange,arguments);
        BindingImpl existingMapping = _bindings.putIfAbsent(b,b);
        if (existingMapping == null || force)
        {
            if (existingMapping != null)
            {
                removeBinding(existingMapping);
            }

            if (b.isDurable() && !restore)
            {
                _virtualHost.getMessageStore().bindQueue(exchange,new AMQShortString(bindingKey),queue,FieldTable.convertToFieldTable(arguments));
            }

            queue.addQueueDeleteTask(b);
            exchange.addCloseTask(b);
            queue.addBinding(b);
            exchange.addBinding(b);
            getConfigStore().addConfiguredObject(b);
            b.logCreation();

            return true;
        }
        else
        {
            return false;
        }
    }

    private ConfigStore getConfigStore()
    {
        return _virtualHost.getConfigStore();
    }

    public void restoreBinding(final String bindingKey, final AMQQueue queue, final Exchange exchange, final Map<String, Object> argumentMap) throws AMQSecurityException, AMQInternalException
    {
        makeBinding(bindingKey,queue,exchange,argumentMap,true, false);
    }

    public void removeBinding(final Binding b) throws AMQSecurityException, AMQInternalException
    {
        removeBinding(b.getBindingKey(), b.getQueue(), b.getExchange(), b.getArguments());
    }


    public Binding removeBinding(String bindingKey, AMQQueue queue, Exchange exchange, Map<String, Object> arguments) throws AMQSecurityException, AMQInternalException
    {
        assert queue != null;
        final Exchange defaultExchange = _virtualHost.getExchangeRegistry().getDefaultExchange();

        if (bindingKey == null)
        {
            bindingKey = "";
        }
        if (exchange == null)
        {
            exchange = defaultExchange;
        }
        if (arguments == null)
        {
            arguments = Collections.emptyMap();
        }

        // The default exchange bindings must reflect the existence of queues, allow
        // all operations on it to succeed. It is up to the broker to prevent illegal
        // attempts at binding to this exchange, not the ACLs.
        if(exchange != defaultExchange)
        {
            // Check access
            if (!_virtualHost.getSecurityManager().authoriseUnbind(exchange, new AMQShortString(bindingKey), queue))
            {
                throw new AMQSecurityException("Permission denied: unbinding " + bindingKey);
            }
        }
        
        BindingImpl b = _bindings.remove(new BindingImpl(bindingKey,queue,exchange,arguments));

        if (b != null)
        {
            exchange.removeBinding(b);
            queue.removeBinding(b);
            exchange.removeCloseTask(b);
            queue.removeQueueDeleteTask(b);

            if (b.isDurable())
            {
                _virtualHost.getMessageStore().unbindQueue(exchange,
                                         new AMQShortString(bindingKey),
                                         queue,
                                         FieldTable.convertToFieldTable(arguments));
            }
            b.logDestruction();
            getConfigStore().removeConfiguredObject(b);
        }

        return b;
    }

    public Binding getBinding(String bindingKey, AMQQueue queue, Exchange exchange, Map<String, Object> arguments)
    {
        assert queue != null;
        final Exchange defaultExchange = _virtualHost.getExchangeRegistry().getDefaultExchange();

        if(bindingKey == null)
        {
            bindingKey = "";
        }
        if(exchange == null)
        {
            exchange = defaultExchange;
        }
        if(arguments == null)
        {
            arguments = Collections.emptyMap();
        }

        BindingImpl b = new BindingImpl(bindingKey,queue,exchange,arguments);
        return _bindings.get(b);
    }
}
