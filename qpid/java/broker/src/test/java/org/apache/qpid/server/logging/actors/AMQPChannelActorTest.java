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
package org.apache.qpid.server.logging.actors;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.qpid.AMQException;
import org.apache.qpid.server.configuration.ServerConfiguration;
import org.apache.qpid.server.logging.rawloggers.UnitTestMessageLogger;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.AMQChannel;

import java.util.List;

/**
 * Test : AMQPChannelActorTest
 * Validate the AMQPChannelActor class.
 *
 * The test creates a new AMQPActor and then logs a message using it.
 *
 * The test then verifies that the logged message was the only one created and
 * that the message contains the required message.
 */
public class AMQPChannelActorTest extends BaseConnectionActorTestCase
{

    AMQChannel _channel;

    @Override
    public void configure()
    {
        super.configure();

        _amqpActor = new AMQPChannelActor(_channel, _rootLogger);
    }

    /**
     * Test that when logging on behalf of the channel
     * The test sends a message then verifies that it entered the logs.
     *
     * The log message should be fully repalaced (no '{n}' values) and should
     * contain the channel id ('/ch:1') identification.
     */
    public void testChannel()
    {
        final String message = sendTestMessage();

        List<Object> logs = _rawLogger.getLogMessages();

        assertEquals("Message log size not as expected.", 1, logs.size());

        // Verify that the logged message is present in the output
        assertTrue("Message was not found in log message:" + logs.get(0),
                   logs.get(0).toString().contains(message));

        // Verify that the message has the correct type
        assertTrue("Message contains the [con: prefix",
                   logs.get(0).toString().contains("[con:"));


        // Verify that all the values were presented to the MessageFormatter
        // so we will not end up with '{n}' entries in the log.
        assertFalse("Verify that the string does not contain any '{'." + logs.get(0),
                    logs.get(0).toString().contains("{"));

        // Verify that the logged message contains the 'ch:1' marker
        assertTrue("Message was not logged as part of channel 1" + logs.get(0),
                   logs.get(0).toString().contains("/ch:1"));

    }

    /**
     * Log a message using the test Actor
     * @return the logged message
     */
    private String sendTestMessage()
    {
        final String message = "test logging";

        _amqpActor.message(new LogSubject()
        {
            public String toString()
            {
                return "[AMQPActorTest]";
            }

        }, new LogMessage()
        {
            public String toString()
            {
                return message;
            }
        });
        return message;
    }

    /**
     * Test that if logging is configured to be off in the configuration that
     * no logging is presented
     * @throws ConfigurationException
     * @throws AMQException
     */
    public void testChannelLoggingOFF() throws ConfigurationException, AMQException
    {
        Configuration config = new PropertiesConfiguration();
        config.addProperty("status-updates", "OFF");

        ServerConfiguration serverConfig = new ServerConfiguration(config);

        _rawLogger = new UnitTestMessageLogger();

        setUpWithConfig(serverConfig);

        sendTestMessage();

        List<Object> logs = _rawLogger.getLogMessages();

        assertEquals("Message log size not as expected.", 0, logs.size());

    }

      /**
     * Test that if logging is configured to be off in the configuration that
     * no logging is presented
     * @throws ConfigurationException
     * @throws AMQException
     */
    public void testChannelLoggingOfF() throws ConfigurationException, AMQException
    {
        Configuration config = new PropertiesConfiguration();
        config.addProperty("status-updates", "OfF");

        ServerConfiguration serverConfig = new ServerConfiguration(config);

        _rawLogger = new UnitTestMessageLogger();

        setUpWithConfig(serverConfig);

        sendTestMessage();

        List<Object> logs = _rawLogger.getLogMessages();

        assertEquals("Message log size not as expected.", 0, logs.size());

    }

    /**
     * Test that if logging is configured to be off in the configuration that
     * no logging is presented
     * @throws ConfigurationException
     * @throws AMQException
     */
    public void testChannelLoggingOff() throws ConfigurationException, AMQException
    {
        Configuration config = new PropertiesConfiguration();
        config.addProperty("status-updates", "Off");

        ServerConfiguration serverConfig = new ServerConfiguration(config);

        _rawLogger = new UnitTestMessageLogger();

        setUpWithConfig(serverConfig);

        sendTestMessage();

        List<Object> logs = _rawLogger.getLogMessages();

        assertEquals("Message log size not as expected.", 0, logs.size());

    }

    /**
     * Test that if logging is configured to be off in the configuration that
     * no logging is presented
     * @throws ConfigurationException
     * @throws AMQException
     */
    public void testChannelLoggingofF() throws ConfigurationException, AMQException
    {
        Configuration config = new PropertiesConfiguration();
        config.addProperty("status-updates", "ofF");

        ServerConfiguration serverConfig = new ServerConfiguration(config);

        _rawLogger = new UnitTestMessageLogger();

        setUpWithConfig(serverConfig);

        sendTestMessage();

        List<Object> logs = _rawLogger.getLogMessages();

        assertEquals("Message log size not as expected.", 0, logs.size());

    }

    /**
     * Test that if logging is configured to be off in the configuration that
     * no logging is presented
     * @throws ConfigurationException
     * @throws AMQException
     */
    public void testChannelLoggingoff() throws ConfigurationException, AMQException
    {
        Configuration config = new PropertiesConfiguration();
        config.addProperty("status-updates", "off");

        ServerConfiguration serverConfig = new ServerConfiguration(config);

        _rawLogger = new UnitTestMessageLogger();

        setUpWithConfig(serverConfig);

        sendTestMessage();

        List<Object> logs = _rawLogger.getLogMessages();

        assertEquals("Message log size not as expected.", 0, logs.size());

    }

    /**
     * Test that if logging is configured to be off in the configuration that
     * no logging is presented
     * @throws ConfigurationException
     * @throws AMQException
     */
    public void testChannelLoggingoFf() throws ConfigurationException, AMQException
    {
        Configuration config = new PropertiesConfiguration();
        config.addProperty("status-updates", "oFf");

        ServerConfiguration serverConfig = new ServerConfiguration(config);

        _rawLogger = new UnitTestMessageLogger();

        setUpWithConfig(serverConfig);

        sendTestMessage();

        List<Object> logs = _rawLogger.getLogMessages();

        assertEquals("Message log size not as expected.", 0, logs.size());

    }

}
