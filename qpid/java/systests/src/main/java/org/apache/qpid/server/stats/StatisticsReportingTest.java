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
package org.apache.qpid.server.stats;

import org.apache.qpid.AMQException;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.exchange.ExchangeDefaults;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.apache.qpid.util.LogMonitor;

import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * Test generation of message/data statistics reporting and the ability
 * to control from the configuration file.
 */
public class StatisticsReportingTest extends QpidBrokerTestCase
{
    private static final String VHOST_NAME1 = "vhost1";
    private static final String VHOST_NAME2 = "vhost2";
    private static final String VHOST_NAME3 = "vhost3";

    protected LogMonitor _monitor;
    protected static final String USER = "admin";

    protected Connection _conToVhost1, _conToVhost2, _conToVhost3;
    protected String _queueName = "statistics";
    protected Destination _queue;
    protected String _brokerUrl;

    @Override
    public void setUp() throws Exception
    {
        createTestVirtualHost(VHOST_NAME1);
        createTestVirtualHost(VHOST_NAME2);
        createTestVirtualHost(VHOST_NAME3);

        setConfigurationProperty("statistics.generation.broker", "true");
        setConfigurationProperty("statistics.generation.virtualhosts", "true");

        if (getName().equals("testEnabledStatisticsReporting"))
        {
            setConfigurationProperty("statistics.reporting.period", "10");
        }

        _monitor = new LogMonitor(_outputFile);

        super.setUp();

        _brokerUrl = getBroker().toString();
        _conToVhost1 = new AMQConnection(_brokerUrl, USER, USER, "clientid", VHOST_NAME1);
        _conToVhost2 = new AMQConnection(_brokerUrl, USER, USER, "clientid", VHOST_NAME2);
        _conToVhost3 = new AMQConnection(_brokerUrl, USER, USER, "clientid", VHOST_NAME3);

        _conToVhost1.start();
        _conToVhost2.start();
        _conToVhost3.start();
    }

    @Override
    public void tearDown() throws Exception
    {
        _conToVhost1.close();
        _conToVhost2.close();
        _conToVhost3.close();

        super.tearDown();
    }

    /**
     * Test enabling reporting.
     */
    public void testEnabledStatisticsReporting() throws Exception
    {
        sendUsing(_conToVhost1, 10, 100);
        sendUsing(_conToVhost2, 20, 100);
        sendUsing(_conToVhost3, 15, 100);

        Thread.sleep(10 * 1000); // 15s

        List<String> brokerStatsData = _monitor.findMatches("BRK-1008");
        List<String> brokerStatsMessages = _monitor.findMatches("BRK-1009");
        List<String> vhostStatsData = _monitor.findMatches("VHT-1003");
        List<String> vhostStatsMessages = _monitor.findMatches("VHT-1004");

        assertEquals("Incorrect number of broker data stats log messages", 2, brokerStatsData.size());
        assertEquals("Incorrect number of broker message stats log messages", 2, brokerStatsMessages.size());
        assertEquals("Incorrect number of virtualhost data stats log messages", 6, vhostStatsData.size());
        assertEquals("Incorrect number of virtualhost message stats log messages", 6, vhostStatsMessages.size());
    }

    /**
     * Test not enabling reporting.
     */
    public void testNotEnabledStatisticsReporting() throws Exception
    {
        sendUsing(_conToVhost1, 10, 100);
        sendUsing(_conToVhost2, 20, 100);
        sendUsing(_conToVhost3, 15, 100);

        Thread.sleep(10 * 1000); // 15s

        List<String> brokerStatsData = _monitor.findMatches("BRK-1008");
        List<String> brokerStatsMessages = _monitor.findMatches("BRK-1009");
        List<String> vhostStatsData = _monitor.findMatches("VHT-1003");
        List<String> vhostStatsMessages = _monitor.findMatches("VHT-1004");

        assertEquals("Incorrect number of broker data stats log messages", 0, brokerStatsData.size());
        assertEquals("Incorrect number of broker message stats log messages", 0, brokerStatsMessages.size());
        assertEquals("Incorrect number of virtualhost data stats log messages", 0, vhostStatsData.size());
        assertEquals("Incorrect number of virtualhost message stats log messages", 0, vhostStatsMessages.size());
    }

    private void sendUsing(Connection con, int number, int size) throws Exception
    {
        Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
        createQueue(session);
        MessageProducer producer = session.createProducer(_queue);
        String content = new String(new byte[size]);
        TextMessage msg = session.createTextMessage(content);
        for (int i = 0; i < number; i++)
        {
            producer.send(msg);
        }
    }

    private void createQueue(Session session) throws AMQException, JMSException
    {
        _queue = new AMQQueue(ExchangeDefaults.DIRECT_EXCHANGE_NAME, _queueName);
        if (!((AMQSession<?,?>) session).isQueueBound((AMQDestination) _queue))
        {
            ((AMQSession<?,?>) session).createQueue(new AMQShortString(_queueName), false, true, false, null);
            ((AMQSession<?,?>) session).declareAndBind((AMQDestination) new AMQQueue(ExchangeDefaults.DIRECT_EXCHANGE_NAME, _queueName));
        }
    }
}
