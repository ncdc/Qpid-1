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
package org.apache.qpid.server.exchange;

import org.apache.log4j.Logger;

import org.apache.qpid.AMQException;
import org.apache.qpid.AMQSecurityException;
import org.apache.qpid.AMQUnknownExchangeType;
import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.server.configuration.VirtualHostConfiguration;
import org.apache.qpid.server.model.UUIDGenerator;
import org.apache.qpid.server.registry.ApplicationRegistry;
import org.apache.qpid.server.virtualhost.VirtualHost;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DefaultExchangeFactory implements ExchangeFactory
{
    private static final Logger _logger = Logger.getLogger(DefaultExchangeFactory.class);
    public static final String DEFAULT_DLE_NAME_SUFFIX = "_DLE";

    private Map<AMQShortString, ExchangeType<? extends Exchange>> _exchangeClassMap = new HashMap<AMQShortString, ExchangeType<? extends Exchange>>();
    private final VirtualHost _host;

    public DefaultExchangeFactory(VirtualHost host)
    {
        _host = host;
        registerExchangeType(DirectExchange.TYPE);
        registerExchangeType(TopicExchange.TYPE);
        registerExchangeType(HeadersExchange.TYPE);
        registerExchangeType(FanoutExchange.TYPE);
    }

    public void registerExchangeType(ExchangeType<? extends Exchange> type)
    {
        _exchangeClassMap.put(type.getName(), type);
    }

    public Collection<ExchangeType<? extends Exchange>> getRegisteredTypes()
    {
        return _exchangeClassMap.values();
    }
    
    public Collection<ExchangeType<? extends Exchange>> getPublicCreatableTypes()
    {
        Collection<ExchangeType<? extends Exchange>> publicTypes = 
                                new ArrayList<ExchangeType<? extends Exchange>>();
        publicTypes.addAll(_exchangeClassMap.values());
        
        return publicTypes;
    }

    public Exchange createExchange(String exchange, String type, boolean durable, boolean autoDelete)
    throws AMQException
    {
        return createExchange(new AMQShortString(exchange), new AMQShortString(type), durable, autoDelete, 0);
    }

    public Exchange createExchange(UUID id, String exchange, String type, boolean durable, boolean autoDelete)
            throws AMQException
    {
        return createExchange(id, new AMQShortString(exchange), new AMQShortString(type), durable, autoDelete, 0);
    }

    public Exchange createExchange(AMQShortString exchange, AMQShortString type, boolean durable,
                                   boolean autoDelete, int ticket)
            throws AMQException
    {
        UUID id = UUIDGenerator.generateExchangeUUID(exchange.asString(), _host.getName());
        return createExchange(id, exchange, type, durable, autoDelete, ticket);
    }

    public Exchange createExchange(UUID id, AMQShortString exchange, AMQShortString type, boolean durable,
                                   boolean autoDelete, int ticket)
            throws AMQException
    {
        // Check access
        if (!_host.getSecurityManager().authoriseCreateExchange(autoDelete, durable, exchange, null, null, null, type))
        {
            String description = "Permission denied: exchange-name '" + exchange.asString() + "'";
            throw new AMQSecurityException(description);
        }
        
        ExchangeType<? extends Exchange> exchType = _exchangeClassMap.get(type);
        if (exchType == null)
        {
            throw new AMQUnknownExchangeType("Unknown exchange type: " + type,null);
        }
        
        Exchange e = exchType.newInstance(id, _host, exchange, durable, ticket, autoDelete);
        return e;
    }

    public void initialise(VirtualHostConfiguration hostConfig)
    {

        if (hostConfig == null)
        {
            return;
        }

        for(Object className : hostConfig.getCustomExchanges())
        {
            try
            {
                ExchangeType<?> exchangeType = ApplicationRegistry.getInstance().getPluginManager().getExchanges().get(String.valueOf(className));
                if (exchangeType == null)
                {
                    _logger.error("No such custom exchange class found: \""+String.valueOf(className)+"\"");
                    continue;
                }
                Class<? extends ExchangeType> exchangeTypeClass = exchangeType.getClass();
                ExchangeType<? extends ExchangeType> type = exchangeTypeClass.newInstance();
                registerExchangeType(type);
            }
            catch (ClassCastException classCastEx)
            {
                _logger.error("No custom exchange class: \""+String.valueOf(className)+"\" cannot be registered as it does not extend class \""+ExchangeType.class+"\"");
            }
            catch (IllegalAccessException e)
            {
                _logger.error("Cannot create custom exchange class: \""+String.valueOf(className)+"\"",e);
            }
            catch (InstantiationException e)
            {
                _logger.error("Cannot create custom exchange class: \""+String.valueOf(className)+"\"",e);
            }
        }

    }
}
