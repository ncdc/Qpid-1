/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.    
 *
 * 
 */
package org.apache.qpid.test.unit.client.BrokerDetails;

import junit.framework.TestCase;

import org.apache.qpid.client.AMQBrokerDetails;
import org.apache.qpid.jms.BrokerDetails;
import org.apache.qpid.transport.ConnectionSettings;
import org.apache.qpid.url.URLSyntaxException;

public class BrokerDetailsTest extends TestCase
{
    public void testDefaultTCP_NODELAY() throws URLSyntaxException
    {
        String brokerURL = "tcp://localhost:5672";
        AMQBrokerDetails broker = new AMQBrokerDetails(brokerURL);

        assertNull("default value should be null", broker.getProperty(BrokerDetails.OPTIONS_TCP_NO_DELAY));
    }

    public void testOverridingTCP_NODELAY() throws URLSyntaxException
    {
        String brokerURL = "tcp://localhost:5672?tcp_nodelay='true'";
        AMQBrokerDetails broker = new AMQBrokerDetails(brokerURL);

        assertTrue("value should be true", Boolean.valueOf(broker.getProperty(BrokerDetails.OPTIONS_TCP_NO_DELAY)));

        brokerURL = "tcp://localhost:5672?tcp_nodelay='false''&maxprefetch='1'";
        broker = new AMQBrokerDetails(brokerURL);

        assertFalse("value should be false", Boolean.valueOf(broker.getProperty(BrokerDetails.OPTIONS_TCP_NO_DELAY)));
    }

    public void testDefaultConnectTimeout() throws URLSyntaxException
    {
        String brokerURL = "tcp://localhost:5672";
        AMQBrokerDetails broker = new AMQBrokerDetails(brokerURL);

        ConnectionSettings settings = broker.buildConnectionSettings();

        assertEquals("unexpected default connect timeout value", BrokerDetails.DEFAULT_CONNECT_TIMEOUT, settings.getConnectTimeout());
    }

    public void testOverridingConnectTimeout() throws URLSyntaxException
    {
        int timeout = 2 * BrokerDetails.DEFAULT_CONNECT_TIMEOUT;
        assertTrue(timeout != BrokerDetails.DEFAULT_CONNECT_TIMEOUT);

        String brokerURL = "tcp://localhost:5672?" + BrokerDetails.OPTIONS_CONNECT_TIMEOUT + "='" + timeout + "'";
        AMQBrokerDetails broker = new AMQBrokerDetails(brokerURL);

        ConnectionSettings settings = broker.buildConnectionSettings();

        assertEquals("unexpected connect timeout value", timeout, settings.getConnectTimeout());
    }

    public void testMultiParameters() throws URLSyntaxException
    {
        String url = "tcp://localhost:5672?timeout='200',immediatedelivery='true'";

        AMQBrokerDetails broker = new AMQBrokerDetails(url);

        assertTrue(broker.getProperty("timeout").equals("200"));
        assertTrue(broker.getProperty("immediatedelivery").equals("true"));
    }

    public void testTransportsDefaultToTCP() throws URLSyntaxException
    {
        String url = "localhost:5672";

        AMQBrokerDetails broker = new AMQBrokerDetails(url);
        assertTrue(broker.getTransport().equals("tcp"));
    }

    public void testCheckDefaultPort() throws URLSyntaxException
    {
        String url = "tcp://localhost";

        AMQBrokerDetails broker = new AMQBrokerDetails(url);
        assertTrue(broker.getPort() == AMQBrokerDetails.DEFAULT_PORT);
    }

    public void testBothDefaults() throws URLSyntaxException
    {
        String url = "localhost";

        AMQBrokerDetails broker = new AMQBrokerDetails(url);

        assertTrue(broker.getTransport().equals("tcp"));
        assertTrue(broker.getPort() == AMQBrokerDetails.DEFAULT_PORT);
    }

    public void testWrongOptionSeparatorInBroker()
    {
        String url = "tcp://localhost:5672+option='value'";
        try
        {
            new AMQBrokerDetails(url);
        }
        catch (URLSyntaxException urise)
        {
            assertTrue(urise.getReason().equals("Illegal character in port number"));
        }
    }

    public void testToStringMasksKeyStorePassword() throws Exception
    {
        String url = "tcp://localhost:5672?key_store_password='password'";
        BrokerDetails details = new AMQBrokerDetails(url);

        String expectedToString = "tcp://localhost:5672?key_store_password='********'";
        String actualToString = details.toString();

        assertEquals("Unexpected toString", expectedToString, actualToString);
    }

    public void testToStringMasksTrustStorePassword() throws Exception
    {
        String url = "tcp://localhost:5672?trust_store_password='password'";
        BrokerDetails details = new AMQBrokerDetails(url);

        String expectedToString = "tcp://localhost:5672?trust_store_password='********'";
        String actualToString = details.toString();

        assertEquals("Unexpected toString", expectedToString, actualToString);
    }

    public void testDefaultSsl() throws URLSyntaxException
    {
        String brokerURL = "tcp://localhost:5672";
        AMQBrokerDetails broker = new AMQBrokerDetails(brokerURL);

        assertNull("default value should be null", broker.getProperty(BrokerDetails.OPTIONS_SSL));
    }

    public void testOverridingSsl() throws URLSyntaxException
    {
        String brokerURL = "tcp://localhost:5672?ssl='true'";
        AMQBrokerDetails broker = new AMQBrokerDetails(brokerURL);

        assertTrue("value should be true", Boolean.valueOf(broker.getProperty(BrokerDetails.OPTIONS_SSL)));

        brokerURL = "tcp://localhost:5672?ssl='false''&maxprefetch='1'";
        broker = new AMQBrokerDetails(brokerURL);

        assertFalse("value should be false", Boolean.valueOf(broker.getProperty(BrokerDetails.OPTIONS_SSL)));
    }
}
