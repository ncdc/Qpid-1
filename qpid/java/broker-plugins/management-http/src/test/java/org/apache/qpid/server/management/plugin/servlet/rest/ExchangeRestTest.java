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
package org.apache.qpid.server.management.plugin.servlet.rest;

import java.net.URLDecoder;
import java.util.List;
import java.util.Map;

import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.Exchange;

public class ExchangeRestTest extends QpidRestTestCase
{
    public void testGet() throws Exception
    {
        List<Map<String, Object>> exchanges = getRestTestHelper().getJsonAsList("/rest/exchange");
        assertNotNull("Exchanges cannot be null", exchanges);
        assertTrue("Unexpected number of exchanges", exchanges.size() >= EXPECTED_HOSTS.length * EXPECTED_EXCHANGES.length);
        for (Map<String, Object> exchange : exchanges)
        {
            Asserts.assertExchange((String) exchange.get(Exchange.NAME), (String) exchange.get(Exchange.TYPE), exchange);
        }
    }

    public void testGetHostExchanges() throws Exception
    {
        List<Map<String, Object>> exchanges = getRestTestHelper().getJsonAsList("/rest/exchange/test");
        assertNotNull("Users cannot be null", exchanges);
        assertEquals("Unexpected number of exchanges", exchanges.size(), EXPECTED_EXCHANGES.length);
        for (String exchangeName : EXPECTED_EXCHANGES)
        {
            Map<String, Object> exchange = getRestTestHelper().find(Exchange.NAME, exchangeName, exchanges);
            assertExchange(exchangeName, exchange);
        }
    }

    public void testGetHostExchangeByName() throws Exception
    {
        for (String exchangeName : EXPECTED_EXCHANGES)
        {
            Map<String, Object> exchange = getRestTestHelper().getJsonAsSingletonList("/rest/exchange/test/"
                    + URLDecoder.decode(exchangeName, "UTF-8"));
            assertExchange(exchangeName, exchange);
        }
    }

    private void assertExchange(String exchangeName, Map<String, Object> exchange)
    {
        assertNotNull("Exchange with name " + exchangeName + " is not found", exchange);
        String type = (String) exchange.get(Exchange.TYPE);
        Asserts.assertExchange(exchangeName, type, exchange);
        if ("direct".equals(type))
        {
            assertBindings(exchange);
        }
    }

    private void assertBindings(Map<String, Object> exchange)
    {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> bindings = (List<Map<String, Object>>) exchange.get("bindings");
        for (String queueName : EXPECTED_QUEUES)
        {
            Map<String, Object> binding = getRestTestHelper().find(Binding.NAME, queueName, bindings);
            Asserts.assertBinding(queueName, (String) exchange.get(Exchange.NAME), binding);
        }
    }

}
