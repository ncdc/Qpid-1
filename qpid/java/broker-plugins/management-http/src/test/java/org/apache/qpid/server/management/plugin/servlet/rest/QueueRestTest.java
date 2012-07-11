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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;

public class QueueRestTest extends QpidRestTestCase
{
    private static final String QUEUE_ATTRIBUTE_CONSUMERS = "consumers";
    private static final String QUEUE_ATTRIBUTE_BINDINGS = "bindings";

    /**
     * Message number to publish into queue
     */
    private static final int MESSAGE_NUMBER = 2;
    private static final int MESSAGE_PAYLOAD_SIZE = 6;
    private static final int ENQUEUED_MESSAGES = 1;
    private static final int DEQUEUED_MESSAGES = 1;
    private static final int ENQUEUED_BYTES = MESSAGE_PAYLOAD_SIZE;
    private static final int DEQUEUED_BYTES = MESSAGE_PAYLOAD_SIZE;

    private Connection _connection;

    public void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection();
        Session session = _connection.createSession(true, Session.SESSION_TRANSACTED);
        String queueName = getTestQueueName();
        Destination queue = session.createQueue(queueName);
        MessageConsumer consumer = session.createConsumer(queue);
        MessageProducer producer = session.createProducer(queue);

        for (int i = 0; i < MESSAGE_NUMBER; i++)
        {
            producer.send(session.createTextMessage("Test-" + i));
        }
        session.commit();
        _connection.start();
        Message m = consumer.receive(1000l);
        assertNotNull("Message is not received", m);
        session.commit();
    }

    public void testGetVirtualHostQueues() throws Exception
    {
        String queueName = getTestQueueName();
        List<Map<String, Object>> queues = getJsonAsList("/rest/queue/test");
        assertEquals("Unexpected number of queues", EXPECTED_QUEUES.length + 1, queues.size());
        String[] expectedQueues = new String[EXPECTED_QUEUES.length + 1];
        System.arraycopy(EXPECTED_QUEUES, 0, expectedQueues, 0, EXPECTED_QUEUES.length);
        expectedQueues[EXPECTED_QUEUES.length] = queueName;

        for (String name : expectedQueues)
        {
            Map<String, Object> queueDetails = find(Queue.NAME, name, queues);
            Asserts.assertQueue(name, "standard", queueDetails);

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> bindings = (List<Map<String, Object>>) queueDetails.get(QUEUE_ATTRIBUTE_BINDINGS);
            assertNotNull("Queue bindings are not found", bindings);
            assertEquals("Unexpected number of bindings", 2, bindings.size());

            Map<String, Object> defaultExchangeBinding = find(Binding.EXCHANGE, "<<default>>", bindings);
            Map<String, Object> directExchangeBinding = find(Binding.EXCHANGE, "amq.direct", bindings);
            Asserts.assertBinding(name, "<<default>>", defaultExchangeBinding);
            Asserts.assertBinding(name, "amq.direct", directExchangeBinding);
        }
    }

    public void testGetByName() throws Exception
    {
        String queueName = getTestQueueName();
        Map<String, Object> queueDetails = getJsonAsSingletonList("/rest/queue/test/" + queueName);
        Asserts.assertQueue(queueName, "standard", queueDetails);
        assertStatistics(queueDetails);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> bindings = (List<Map<String, Object>>) queueDetails.get(QUEUE_ATTRIBUTE_BINDINGS);
        assertNotNull("Queue bindings are not found", bindings);
        assertEquals("Unexpected number of bindings", 2, bindings.size());

        Map<String, Object> defaultExchangeBinding = find(Binding.EXCHANGE, "<<default>>", bindings);
        Map<String, Object> directExchangeBinding = find(Binding.EXCHANGE, "amq.direct", bindings);
        Asserts.assertBinding(queueName, "<<default>>", defaultExchangeBinding);
        Asserts.assertBinding(queueName, "amq.direct", directExchangeBinding);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> consumers = (List<Map<String, Object>>) queueDetails.get(QUEUE_ATTRIBUTE_CONSUMERS);
        assertNotNull("Queue consumers are not found", consumers);
        assertEquals("Unexpected number of consumers", 1, consumers.size());
        assertConsumer(consumers.get(0));
    }

    public void testPutCreateBinding() throws Exception
    {
        String queueName = getTestQueueName();
        String bindingName = queueName + 2;
        String[] exchanges = { "amq.direct", "amq.fanout", "amq.topic", "amq.match", "qpid.management", "<<default>>" };

        for (int i = 0; i < exchanges.length; i++)
        {
            createBinding(bindingName, exchanges[i], queueName);
        }

        Map<String, Object> queueDetails = getJsonAsSingletonList("/rest/queue/test/" + queueName);
        Asserts.assertQueue(queueName, "standard", queueDetails);

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> bindings = (List<Map<String, Object>>) queueDetails.get(QUEUE_ATTRIBUTE_BINDINGS);
        assertNotNull("Queue bindings are not found", bindings);
        assertEquals("Unexpected number of bindings", exchanges.length + 2, bindings.size());

        Map<String, Object> searchAttributes = new HashMap<String, Object>();
        searchAttributes.put(Binding.NAME, bindingName);

        for (int i = 0; i < exchanges.length; i++)
        {
            searchAttributes.put(Binding.EXCHANGE, exchanges[i]);
            Map<String, Object> binding = find(searchAttributes, bindings);
            Asserts.assertBinding(bindingName, queueName, exchanges[i], binding);
        }
    }

    private void createBinding(String bindingName, String exchangeName, String queueName) throws IOException
    {
        HttpURLConnection connection = openManagementConection(
                "/rest/binding/test/" + URLDecoder.decode(exchangeName, "UTF-8") + "/" + queueName + "/" + bindingName,
                "PUT");

        Map<String, Object> bindingData = new HashMap<String, Object>();
        bindingData.put(Binding.NAME, bindingName);
        bindingData.put(Binding.EXCHANGE, exchangeName);
        bindingData.put(Binding.QUEUE, queueName);

        writeJsonRequest(connection, bindingData);
        assertEquals("Unexpected response code", 201, connection.getResponseCode());

        connection.disconnect();
    }

    private void assertConsumer(Map<String, Object> consumer)
    {
        assertNotNull("Consumer map should not be null", consumer);
        Asserts.assertAttributesPresent(consumer, Consumer.AVAILABLE_ATTRIBUTES, Consumer.STATE, Consumer.TIME_TO_LIVE,
                Consumer.CREATED, Consumer.UPDATED, Consumer.SETTLEMENT_MODE, Consumer.EXCLUSIVE, Consumer.SELECTOR,
                Consumer.NO_LOCAL);

        assertEquals("Unexpected binding attribute " + Consumer.NAME, "1", consumer.get(Consumer.NAME));
        assertEquals("Unexpected binding attribute " + Consumer.DURABLE, Boolean.FALSE, consumer.get(Consumer.DURABLE));
        assertEquals("Unexpected binding attribute " + Consumer.LIFETIME_POLICY, LifetimePolicy.AUTO_DELETE.name(),
                consumer.get(Consumer.LIFETIME_POLICY));
        assertEquals("Unexpected binding attribute " + Consumer.DISTRIBUTION_MODE, "MOVE",
                consumer.get(Consumer.DISTRIBUTION_MODE));

        @SuppressWarnings("unchecked")
        Map<String, Object> statistics = (Map<String, Object>) consumer.get(Asserts.STATISTICS_ATTRIBUTE);
        assertNotNull("Consumer statistics is not present", statistics);
        Asserts.assertAttributesPresent(statistics, Consumer.AVAILABLE_STATISTICS, Consumer.STATE_CHANGED);
    }

    private void assertStatistics(Map<String, Object> queueDetails)
    {
        @SuppressWarnings("unchecked")
        Map<String, Object> statistics = (Map<String, Object>) queueDetails.get(Asserts.STATISTICS_ATTRIBUTE);
        assertEquals("Unexpected queue statistics attribute " + Queue.PERSISTENT_DEQUEUED_MESSAGES, DEQUEUED_MESSAGES,
                statistics.get(Queue.PERSISTENT_DEQUEUED_MESSAGES));
        assertEquals("Unexpected queue statistics attribute " + Queue.QUEUE_DEPTH_MESSAGES, ENQUEUED_MESSAGES,
                statistics.get(Queue.QUEUE_DEPTH_MESSAGES));
        assertEquals("Unexpected queue statistics attribute " + Queue.CONSUMER_COUNT, 1,
                statistics.get(Queue.CONSUMER_COUNT));
        assertEquals("Unexpected queue statistics attribute " + Queue.CONSUMER_COUNT_WITH_CREDIT, 1,
                statistics.get(Queue.CONSUMER_COUNT_WITH_CREDIT));
        assertEquals("Unexpected queue statistics attribute " + Queue.BINDING_COUNT, 2, statistics.get(Queue.BINDING_COUNT));
        assertEquals("Unexpected queue statistics attribute " + Queue.PERSISTENT_DEQUEUED_MESSAGES, DEQUEUED_MESSAGES,
                statistics.get(Queue.PERSISTENT_DEQUEUED_MESSAGES));
        assertEquals("Unexpected queue statistics attribute " + Queue.TOTAL_DEQUEUED_MESSAGES, DEQUEUED_MESSAGES,
                statistics.get(Queue.TOTAL_DEQUEUED_MESSAGES));
        assertEquals("Unexpected queue statistics attribute " + Queue.TOTAL_DEQUEUED_BYTES, DEQUEUED_BYTES,
                statistics.get(Queue.TOTAL_DEQUEUED_BYTES));
        assertEquals("Unexpected queue statistics attribute " + Queue.PERSISTENT_DEQUEUED_BYTES, DEQUEUED_BYTES,
                statistics.get(Queue.TOTAL_DEQUEUED_BYTES));
        assertEquals("Unexpected queue statistics attribute " + Queue.PERSISTENT_ENQUEUED_BYTES, ENQUEUED_BYTES
                + DEQUEUED_BYTES, statistics.get(Queue.PERSISTENT_ENQUEUED_BYTES));
        assertEquals("Unexpected queue statistics attribute " + Queue.TOTAL_ENQUEUED_BYTES, ENQUEUED_BYTES + DEQUEUED_BYTES,
                statistics.get(Queue.TOTAL_ENQUEUED_BYTES));
        assertEquals("Unexpected queue statistics attribute " + Queue.QUEUE_DEPTH_BYTES, ENQUEUED_BYTES,
                statistics.get(Queue.QUEUE_DEPTH_BYTES));
    }
}
