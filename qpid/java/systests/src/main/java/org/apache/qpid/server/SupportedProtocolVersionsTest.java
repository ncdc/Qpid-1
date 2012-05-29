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
package org.apache.qpid.server;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.framing.ProtocolVersion;
import org.apache.qpid.server.configuration.ServerConfiguration;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

/**
 * Tests to validate it is possible to disable support for particular protocol
 * versions entirely, rather than selectively excluding them on particular ports,
 * and it is possible to configure the reply to an unsupported protocol initiation.
 */
public class SupportedProtocolVersionsTest extends QpidBrokerTestCase
{
    public void setUp() throws Exception
    {
        // No-op, we call super.setUp() from test methods after appropriate config overrides
    }

    private void clearProtocolSupportManipulations()
    {
        //Remove the QBTC provided protocol manipulations, giving only the protocols which default to enabled
        setTestSystemProperty(QpidBrokerTestCase.BROKER_PROTOCOL_EXCLUDES, null);
        setTestSystemProperty(QpidBrokerTestCase.BROKER_PROTOCOL_INCLUDES, null);
    }

    /**
     * Test that 0-10, 0-9-1, 0-9, and 0-8 support is present when no
     * attempt has yet been made to disable them, and forcing the client
     * to negotiate from a particular protocol version returns a connection
     * using the expected protocol version.
     */
    public void testDefaultProtocolSupport() throws Exception
    {
        clearProtocolSupportManipulations();

        super.setUp();

        //Verify requesting a 0-10 connection works
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-10");
        AMQConnection connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v0_10, connection.getProtocolVersion());
        connection.close();

        //Verify requesting a 0-91 connection works
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-9-1");
        connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v0_91, connection.getProtocolVersion());
        connection.close();

        //Verify requesting a 0-9 connection works
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-9");
        connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v0_9, connection.getProtocolVersion());
        connection.close();

        //Verify requesting a 0-8 connection works
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-8");
        connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v8_0, connection.getProtocolVersion());
        connection.close();
    }

    public void testDisabling010and10() throws Exception
    {
        clearProtocolSupportManipulations();

        //disable 0-10 and 1-0 support
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP010ENABLED, "false");
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP10ENABLED, "false");

        super.setUp();

        //Verify initially requesting a 0-10 connection now negotiates a 0-91
        //connection as the broker should reply with its highest supported protocol
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-10");
        AMQConnection connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v0_91, connection.getProtocolVersion());
        connection.close();
    }

    public void testDisabling091and010and10() throws Exception
    {
        clearProtocolSupportManipulations();

        //disable 0-91 and 0-10 and 1-0 support
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP10ENABLED, "false");
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP010ENABLED, "false");
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP091ENABLED, "false");

        super.setUp();

        //Verify initially requesting a 0-10 connection now negotiates a 0-9
        //connection as the broker should reply with its highest supported protocol
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-10");
        AMQConnection connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v0_9, connection.getProtocolVersion());
        connection.close();
    }

    public void testDisabling09and091and010and10() throws Exception
    {
        clearProtocolSupportManipulations();

        //disable 0-9, 0-91, 0-10 and 1-0 support
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP09ENABLED, "false");
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP091ENABLED, "false");
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP010ENABLED, "false");
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP10ENABLED, "false");

        super.setUp();

        //Verify initially requesting a 0-10 connection now negotiates a 0-8
        //connection as the broker should reply with its highest supported protocol
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-10");
        AMQConnection connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v8_0, connection.getProtocolVersion());
        connection.close();
    }

    public void testConfiguringReplyingToUnsupported010ProtocolInitiationWith09insteadOf091() throws Exception
    {
        clearProtocolSupportManipulations();

        //disable 0-10 support, and set the default unsupported protocol initiation reply to 0-9
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP010ENABLED, "false");
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP_SUPPORTED_REPLY, "v0_9");

        super.setUp();

        //Verify initially requesting a 0-10 connection now negotiates a 0-9 connection as the
        //broker should reply with its 'default unsupported protocol initiation reply' as opposed
        //to the previous behaviour of the highest supported protocol version.
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-10");
        AMQConnection connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v0_9, connection.getProtocolVersion());
        connection.close();

        //Verify requesting a 0-91 connection directly still works, as its support is still enabled
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-9-1");
        connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v0_91, connection.getProtocolVersion());
        connection.close();
    }

    public void testProtocolInclusionThroughQBTCSystemPropertiesOverridesProtocolExclusion() throws Exception
    {
        testProtocolInclusionOverridesProtocolExclusion(false);
    }

    public void testProtocolInclusionThroughConfigOverridesProtocolExclusion() throws Exception
    {
        testProtocolInclusionOverridesProtocolExclusion(true);
    }

    private void testProtocolInclusionOverridesProtocolExclusion(boolean useConfig) throws Exception
    {
        clearProtocolSupportManipulations();

        //selectively exclude 0-10 and 1-0 on the test port
        setTestSystemProperty(QpidBrokerTestCase.BROKER_PROTOCOL_EXCLUDES,"--exclude-0-10 @PORT --exclude-1-0 @PORT");

        super.setUp();

        //Verify initially requesting a 0-10 connection negotiates a 0-9-1 connection
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-10");
        AMQConnection connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v0_91, connection.getProtocolVersion());
        connection.close();

        stopBroker();

        if(useConfig)
        {
            //selectively include 0-10 support again on the test port through config
            setConfigurationProperty(ServerConfiguration.CONNECTOR_INCLUDE_010, String.valueOf(getPort()));
        }
        else
        {
            //selectively include 0-10 support again on the test port through QBTC sys props
            setTestSystemProperty(QpidBrokerTestCase.BROKER_PROTOCOL_INCLUDES,"--include-0-10 @PORT");
        }

        startBroker();

        //Verify requesting a 0-10 connection now returns one
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-10");
        connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v0_10, connection.getProtocolVersion());
        connection.close();
    }

    public void testProtocolInclusionOverridesProtocolDisabling() throws Exception
    {
        clearProtocolSupportManipulations();

        //disable 0-10 and 1-0
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP010ENABLED, "false");
        setConfigurationProperty(ServerConfiguration.CONNECTOR_AMQP10ENABLED, "false");

        //selectively include 0-10 support again on the test port
        setConfigurationProperty(ServerConfiguration.CONNECTOR_INCLUDE_010, String.valueOf(getPort()));

        super.setUp();

        //Verify initially requesting a 0-10 connection still works
        setTestClientSystemProperty(ClientProperties.AMQP_VERSION, "0-10");
        AMQConnection connection = (AMQConnection) getConnection();
        assertEquals("Unexpected protocol version in use", ProtocolVersion.v0_10, connection.getProtocolVersion());
        connection.close();
    }

}