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

import org.apache.qpid.server.AMQChannel;
import org.apache.qpid.server.util.BrokerTestHelper;

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

    public void setUp()
    {
        // do nothing
    }

    private void setUpNow() throws Exception
    {
        super.setUp();
        AMQChannel channel = BrokerTestHelper.createChannel(1, getSession());

        setAmqpActor(new AMQPChannelActor(channel, getRootLogger()));
    }


    /**
     * Test that when logging on behalf of the channel
     * The test sends a message then verifies that it entered the logs.
     *
     * The log message should be fully repalaced (no '{n}' values) and should
     * contain the channel id ('/ch:1') identification.
     */
    public void testChannel() throws Exception
    {
        setUpNow();

        final String message = sendTestLogMessage(getAmqpActor());

        List<Object> logs = getRawLogger().getLogMessages();

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
     * Test that if logging is configured to be off via system property that
     * no logging is presented
     */
    public void testChannelLoggingOFF() throws Exception
    {
        setStatusUpdatesEnabled(false);

        setUpNow();

        sendTestLogMessage(getAmqpActor());

        List<Object> logs = getRawLogger().getLogMessages();

        assertEquals("Message log size not as expected.", 0, logs.size());
    }
}
