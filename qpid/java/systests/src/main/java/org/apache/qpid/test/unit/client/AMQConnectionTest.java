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
package org.apache.qpid.test.unit.client;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TopicSession;

import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.client.AMQQueue;
import org.apache.qpid.client.AMQSession;
import org.apache.qpid.client.AMQTopic;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQConnectionTest extends QpidBrokerTestCase
{
    protected static AMQConnection _connection;
    protected static AMQTopic _topic;
    protected static AMQQueue _queue;
    private static QueueSession _queueSession;
    private static TopicSession _topicSession;
    protected static final Logger _logger = LoggerFactory.getLogger(AMQConnectionTest.class);

    protected void setUp() throws Exception
    {
        super.setUp();
        createConnection();
        _topic = new AMQTopic(_connection.getDefaultTopicExchangeName(), new AMQShortString("mytopic"));
        _queue = new AMQQueue(_connection.getDefaultQueueExchangeName(), new AMQShortString("myqueue"));
    }

    @Override
    protected void tearDown() throws Exception
    {
        _connection.close();
        super.tearDown();    //To change body of overridden methods use File | Settings | File Templates.
    }

    protected void createConnection() throws Exception
    {
        _connection = (AMQConnection) getConnection("guest", "guest");
    }

    /**
     * Simple tests to check we can create TopicSession and QueueSession ok
     * And that they throw exceptions where appropriate as per JMS spec
     */

    public void testCreateQueueSession() throws JMSException
    {
        createQueueSession();
    }

    private void createQueueSession() throws JMSException
    {
        _queueSession =  _connection.createQueueSession(false, AMQSession.NO_ACKNOWLEDGE);
    }

    public void testCreateTopicSession() throws JMSException
    {
        createTopicSession();
    }

    private void createTopicSession() throws JMSException
    {
        _topicSession = _connection.createTopicSession(false, AMQSession.NO_ACKNOWLEDGE);
    }

    public void testTopicSessionCreateBrowser() throws JMSException
    {
        createTopicSession();
        try
        {
            _topicSession.createBrowser(_queue);
            fail("expected exception did not occur");
        }
        catch (javax.jms.IllegalStateException s)
        {
            // ok
        }
        catch (Exception e)
        {
            fail("expected javax.jms.IllegalStateException, got " + e);
        }
    }

    public void testTopicSessionCreateQueue() throws JMSException
    {
        createTopicSession();
        try
        {
            _topicSession.createQueue("abc");
            fail("expected exception did not occur");
        }
        catch (javax.jms.IllegalStateException s)
        {
            // ok
        }
        catch (Exception e)
        {
            fail("expected javax.jms.IllegalStateException, got " + e);
        }
    }

    public void testTopicSessionCreateTemporaryQueue() throws JMSException
    {
        createTopicSession();
        try
        {
            _topicSession.createTemporaryQueue();
            fail("expected exception did not occur");
        }
        catch (javax.jms.IllegalStateException s)
        {
            // ok
        }
        catch (Exception e)
        {
            fail("expected javax.jms.IllegalStateException, got " + e);
        }
    }

    public void testQueueSessionCreateTemporaryTopic() throws JMSException
    {
        createQueueSession();
        try
        {
            _queueSession.createTemporaryTopic();
            fail("expected exception did not occur");
        }
        catch (javax.jms.IllegalStateException s)
        {
            // ok
        }
        catch (Exception e)
        {
            fail("expected javax.jms.IllegalStateException, got " + e);
        }
    }

    public void testQueueSessionCreateTopic() throws JMSException
    {
        createQueueSession();
        try
        {
            _queueSession.createTopic("abc");
            fail("expected exception did not occur");
        }
        catch (javax.jms.IllegalStateException s)
        {
            // ok
        }
        catch (Exception e)
        {
            fail("expected javax.jms.IllegalStateException, got " + e);
        }
    }

    public void testQueueSessionDurableSubscriber() throws JMSException
    {
        createQueueSession();
        try
        {
            _queueSession.createDurableSubscriber(_topic, "abc");
            fail("expected exception did not occur");
        }
        catch (javax.jms.IllegalStateException s)
        {
            // ok
        }
        catch (Exception e)
        {
            fail("expected javax.jms.IllegalStateException, got " + e);
        }
    }

    public void testQueueSessionUnsubscribe() throws JMSException
    {
        createQueueSession();
        try
        {
            _queueSession.unsubscribe("abc");
            fail("expected exception did not occur");
        }
        catch (javax.jms.IllegalStateException s)
        {
            // ok
        }
        catch (Exception e)
        {
            fail("expected javax.jms.IllegalStateException, got " + e);
        }
    }

    public void testPrefetchSystemProperty() throws Exception
    {
        _connection.close();
        setTestClientSystemProperty(ClientProperties.MAX_PREFETCH_PROP_NAME, new Integer(2).toString());
        
        createConnection();
        _connection.start();
        // Create two consumers on different sessions
        Session consSessA = _connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumerA = consSessA.createConsumer(_queue);

        Session producerSession = _connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = producerSession.createProducer(_queue);

        // Send 3 messages
        for (int i = 0; i < 3; i++)
        {
            producer.send(producerSession.createTextMessage("test"));
        }
        producerSession.commit();
        
        MessageConsumer consumerB = null;
        // 0-8, 0-9, 0-9-1 prefetch is per session, not consumer.
        if (!isBroker010())
        {
            Session consSessB = _connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            consumerB = consSessB.createConsumer(_queue);
        }
        else
        {
            consumerB = consSessA.createConsumer(_queue);
        }

        Message msg;
        // Check that consumer A has 2 messages
        for (int i = 0; i < 2; i++)
        {
            msg = consumerA.receive(1500);
            assertNotNull("Consumer A should receive 2 messages",msg);                
        }
        
        msg = consumerA.receive(1500);
        assertNull("Consumer A should not have received a 3rd message",msg);
        
        // Check that consumer B has the last message
        msg = consumerB.receive(1500);
        assertNotNull("Consumer B should have received the message",msg);
    }
    


}
