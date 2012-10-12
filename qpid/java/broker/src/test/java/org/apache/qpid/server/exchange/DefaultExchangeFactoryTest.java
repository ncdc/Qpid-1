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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.qpid.AMQException;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.server.plugin.ExchangeType;
import org.apache.qpid.server.virtualhost.VirtualHost;
import org.apache.qpid.test.utils.QpidTestCase;

@SuppressWarnings("rawtypes")
public class DefaultExchangeFactoryTest extends QpidTestCase
{
    private DirectExchangeType _directExchangeType;
    private TopicExchangeType _topicExchangeType;
    private FanoutExchangeType _fanoutExchangeType;
    private HeadersExchangeType _headersExchangeType;

    private List<ExchangeType> _stubbedExchangeTypes;

    protected void setUp() throws Exception
    {
        super.setUp();

        _directExchangeType = new DirectExchangeType();
        _topicExchangeType = new TopicExchangeType();
        _fanoutExchangeType = new FanoutExchangeType();
        _headersExchangeType = new HeadersExchangeType();
        _stubbedExchangeTypes = new ArrayList<ExchangeType>();
    }

    public void testCreateDefaultExchangeFactory()
    {
        _stubbedExchangeTypes.add(_directExchangeType);
        _stubbedExchangeTypes.add(_topicExchangeType);
        _stubbedExchangeTypes.add(_fanoutExchangeType);
        _stubbedExchangeTypes.add(_headersExchangeType);

        DefaultExchangeFactory factory = new TestExchangeFactory();

        Collection<ExchangeType<? extends Exchange>> registeredTypes = factory.getRegisteredTypes();
        assertEquals("Unexpected number of exchange types", _stubbedExchangeTypes.size(), registeredTypes.size());
        assertTrue("Direct exchange type is not found", registeredTypes.contains(_directExchangeType));
        assertTrue("Fanout exchange type is not found", registeredTypes.contains(_fanoutExchangeType));
        assertTrue("Topic exchange type is not found", registeredTypes.contains(_topicExchangeType));
        assertTrue("Headers exchange type is not found", registeredTypes.contains(_headersExchangeType));
    }

    public void testCreateDefaultExchangeFactoryWithoutAllBaseExchangeTypes()
    {
        try
        {
            new TestExchangeFactory();
            fail("Cannot create factory without all base classes");
        }
        catch (IllegalStateException e)
        {
            // pass
        }
    }

    public void testCreateDefaultExchangeFactoryWithoutDireactExchangeType()
    {
        _stubbedExchangeTypes.add(_topicExchangeType);
        _stubbedExchangeTypes.add(_fanoutExchangeType);
        _stubbedExchangeTypes.add(_headersExchangeType);

        try
        {
            new TestExchangeFactory();
            fail("Cannot create factory without all base classes");
        }
        catch (IllegalStateException e)
        {
            assertEquals("Unexpected exception message", "Did not find expected exchange type: " + _directExchangeType.getName(), e.getMessage());
        }
    }

    public void testCreateDefaultExchangeFactoryWithoutTopicExchangeType()
    {
        _stubbedExchangeTypes.add(_directExchangeType);
        _stubbedExchangeTypes.add(_fanoutExchangeType);
        _stubbedExchangeTypes.add(_headersExchangeType);

        try
        {
            new TestExchangeFactory();
            fail("Cannot create factory without all base classes");
        }
        catch (IllegalStateException e)
        {
            assertEquals("Unexpected exception message", "Did not find expected exchange type: " + _topicExchangeType.getName(), e.getMessage());
        }
    }

    public void testCreateDefaultExchangeFactoryWithoutFanoutExchangeType()
    {
        _stubbedExchangeTypes.add(_directExchangeType);
        _stubbedExchangeTypes.add(_topicExchangeType);
        _stubbedExchangeTypes.add(_headersExchangeType);

        try
        {
            new TestExchangeFactory();
            fail("Cannot create factory without all base classes");
        }
        catch (IllegalStateException e)
        {
            assertEquals("Unexpected exception message", "Did not find expected exchange type: " + _fanoutExchangeType.getName(), e.getMessage());
        }
    }

    public void testCreateDefaultExchangeFactoryWithoutHeadersExchangeType()
    {
        _stubbedExchangeTypes.add(_directExchangeType);
        _stubbedExchangeTypes.add(_topicExchangeType);
        _stubbedExchangeTypes.add(_fanoutExchangeType);

        try
        {
            new TestExchangeFactory();
            fail("Cannot create factory without all base classes");
        }
        catch (IllegalStateException e)
        {
            assertEquals("Unexpected exception message", "Did not find expected exchange type: " + _headersExchangeType.getName(), e.getMessage());
        }
    }

    public void testCreateDefaultExchangeFactoryWithDuplicateExchangeTypeName()
    {
        _stubbedExchangeTypes.add(_directExchangeType);
        _stubbedExchangeTypes.add(_directExchangeType);

        try
        {
            new TestExchangeFactory();
            fail("Cannot create factory with duplicate exchange type names");
        }
        catch (IllegalStateException e)
        {
            assertTrue( "Unexpected exception message", e.getMessage().contains("ExchangeType with type name '"
                    + _directExchangeType.getName() + "' is already registered using class '"
                    + DirectExchangeType.class.getName()));
        }
    }

    public void testCreateDefaultExchangeFactoryWithCustomExchangeType()
    {
        ExchangeType<?> customeExchangeType = new ExchangeType<Exchange>()
        {
            @Override
            public AMQShortString getName()
            {
                return new AMQShortString("my-custom-exchange");
            }

            @Override
            public Exchange newInstance(UUID id, VirtualHost host, AMQShortString name, boolean durable, int ticket,
                    boolean autoDelete) throws AMQException
            {
                return null;
            }

            @Override
            public AMQShortString getDefaultExchangeName()
            {
                return null;
            }
        };

        _stubbedExchangeTypes.add(customeExchangeType);
        _stubbedExchangeTypes.add(_directExchangeType);
        _stubbedExchangeTypes.add(_topicExchangeType);
        _stubbedExchangeTypes.add(_fanoutExchangeType);
        _stubbedExchangeTypes.add(_headersExchangeType);

        DefaultExchangeFactory factory = new TestExchangeFactory();

        Collection<ExchangeType<? extends Exchange>> registeredTypes = factory.getRegisteredTypes();
        assertEquals("Unexpected number of exchange types", _stubbedExchangeTypes.size(), registeredTypes.size());
        assertTrue("Direct exchange type is not found", registeredTypes.contains(_directExchangeType));
        assertTrue("Fanout exchange type is not found", registeredTypes.contains(_fanoutExchangeType));
        assertTrue("Topic exchange type is not found", registeredTypes.contains(_topicExchangeType));
        assertTrue("Headers exchange type is not found", registeredTypes.contains(_headersExchangeType));
        assertTrue("Custom exchange type is not found", registeredTypes.contains(customeExchangeType));
    }

    private final class TestExchangeFactory extends DefaultExchangeFactory
    {
        private TestExchangeFactory()
        {
            super(null);
        }

        @Override
        protected Iterable<ExchangeType> loadExchangeTypes()
        {
            return _stubbedExchangeTypes;
        }
    }

}
