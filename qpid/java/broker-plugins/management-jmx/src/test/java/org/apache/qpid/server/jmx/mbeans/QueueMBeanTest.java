/*
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
 */
package org.apache.qpid.server.jmx.mbeans;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.argThat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.OperationsException;
import javax.management.openmbean.CompositeDataSupport;

import org.apache.qpid.management.common.mbeans.ManagedQueue;
import org.apache.qpid.server.jmx.ManagedObjectRegistry;
import org.apache.qpid.server.jmx.mbeans.QueueMBean.GetMessageVisitor;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.LifetimePolicy;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Statistics;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.queue.NotificationCheck;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.test.utils.QpidTestCase;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class QueueMBeanTest extends QpidTestCase
{
    private static final String QUEUE_NAME = "QUEUE_NAME";
    private static final String QUEUE_DESCRIPTION = "QUEUE_DESCRIPTION";
    private static final String  QUEUE_TYPE = "QUEUE_TYPE";
    private static final String QUEUE_ALTERNATE_EXCHANGE = "QUEUE_ALTERNATE_EXCHANGE";

    private Queue _mockQueue;
    private Statistics _mockQueueStatistics;
    private VirtualHostMBean _mockVirtualHostMBean;
    private ManagedObjectRegistry _mockManagedObjectRegistry;
    private QueueMBean _queueMBean;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        _mockQueue = mock(Queue.class);
        _mockQueueStatistics = mock(Statistics.class);
        when(_mockQueue.getName()).thenReturn(QUEUE_NAME);
        when(_mockQueue.getStatistics()).thenReturn(_mockQueueStatistics);
        _mockVirtualHostMBean = mock(VirtualHostMBean.class);

        _mockManagedObjectRegistry = mock(ManagedObjectRegistry.class);
        when(_mockVirtualHostMBean.getRegistry()).thenReturn(_mockManagedObjectRegistry);

        _queueMBean = new QueueMBean(_mockQueue, _mockVirtualHostMBean);
    }

    public void testQueueName()
    {
        assertEquals(QUEUE_NAME, _queueMBean.getName());
    }

    /**********  Statistics **********/

    public void testGetMessageCount() throws Exception
    {
        assertStatistic("messageCount", 1000, Queue.QUEUE_DEPTH_MESSAGES);
    }

    public void testGetReceivedMessageCount() throws Exception
    {
        assertStatistic("receivedMessageCount", 1000l, Queue.TOTAL_ENQUEUED_MESSAGES);
    }

    public void testQueueDepth() throws Exception
    {
        assertStatistic("queueDepth", 4096l, Queue.QUEUE_DEPTH_BYTES);
    }

    public void testActiveConsumerCount() throws Exception
    {
        assertStatistic("activeConsumerCount", 3, Queue.CONSUMER_COUNT_WITH_CREDIT);
    }

    public void testConsumerCount() throws Exception
    {
        assertStatistic("consumerCount", 3, Queue.CONSUMER_COUNT);
    }

    /**********  Simple Attributes **********/

    public void testGetQueueDescription() throws Exception
    {
        assertAttribute("description", QUEUE_DESCRIPTION, Queue.DESCRIPTION);
    }

    public void testSetQueueDescription() throws Exception
    {
        testSetAttribute("description", Queue.DESCRIPTION, "descriptionold", "descriptionnew");
    }

    public void testQueueType() throws Exception
    {
        assertAttribute("queueType", QUEUE_TYPE, Queue.TYPE);
    }

    public void testMaximumDeliveryCount() throws Exception
    {
        assertAttribute("maximumDeliveryCount", 5, Queue.MAXIMUM_DELIVERY_ATTEMPTS);
    }

    public void testOwner() throws Exception
    {
        assertAttribute("owner", "testOwner", Queue.OWNER);
    }

    public void testIsDurable() throws Exception
    {
        when(_mockQueue.isDurable()).thenReturn(true);
        assertTrue(_queueMBean.isDurable());
    }

    public void testIsNotDurable() throws Exception
    {
        when(_mockQueue.isDurable()).thenReturn(false);
        assertFalse(_queueMBean.isDurable());
    }

    public void testIsAutoDelete() throws Exception
    {
        when(_mockQueue.getLifetimePolicy()).thenReturn(LifetimePolicy.AUTO_DELETE);
        assertTrue(_queueMBean.isAutoDelete());
    }

    public void testIsNotAutoDelete() throws Exception
    {
        when(_mockQueue.getLifetimePolicy()).thenReturn(LifetimePolicy.PERMANENT);
        assertFalse(_queueMBean.isAutoDelete());
    }

    public void testGetMaximumMessageAge() throws Exception
    {
        assertAttribute("maximumMessageAge", 10000l, Queue.ALERT_THRESHOLD_MESSAGE_AGE);
    }

    public void testSetMaximumMessageAge() throws Exception
    {
        testSetAttribute("maximumMessageAge", Queue.ALERT_THRESHOLD_MESSAGE_AGE, 1000l, 10000l);
    }

    public void testGetMaximumMessageSize() throws Exception
    {
        assertAttribute("maximumMessageSize", 1024l, Queue.ALERT_THRESHOLD_MESSAGE_SIZE);
    }

    public void testSetMaximumMessageSize() throws Exception
    {
        testSetAttribute("maximumMessageSize", Queue.ALERT_THRESHOLD_MESSAGE_SIZE, 1024l, 2048l);
    }

    public void testGetMaximumMessageCount() throws Exception
    {
        assertAttribute("maximumMessageCount", 5000l, Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES);
    }

    public void testSetMaximumMessageCount() throws Exception
    {
        testSetAttribute("maximumMessageCount", Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 4000l, 5000l);
    }

    public void testGetMaximumQueueDepth() throws Exception
    {
        assertAttribute("maximumQueueDepth", 1048576l, Queue.ALERT_THRESHOLD_QUEUE_DEPTH_BYTES);
    }

    public void testSetMaximumQueueDepth() throws Exception
    {
        testSetAttribute("maximumQueueDepth", Queue.ALERT_THRESHOLD_QUEUE_DEPTH_BYTES,1048576l , 2097152l);
    }

    public void testGetCapacity() throws Exception
    {
        assertAttribute("capacity", 1048576l, Queue.QUEUE_FLOW_CONTROL_SIZE_BYTES);
    }

    public void testSetCapacity() throws Exception
    {
        testSetAttribute("capacity", Queue.QUEUE_FLOW_CONTROL_SIZE_BYTES,1048576l , 2097152l);
    }

    public void testGetFlowResumeCapacity() throws Exception
    {
        assertAttribute("flowResumeCapacity", 1048576l, Queue.QUEUE_FLOW_RESUME_SIZE_BYTES);
    }

    public void testSetFlowResumeCapacity() throws Exception
    {
        testSetAttribute("flowResumeCapacity", Queue.QUEUE_FLOW_RESUME_SIZE_BYTES,1048576l , 2097152l);
    }

    public void testIsExclusive() throws Exception
    {
        assertAttribute("exclusive", Boolean.TRUE, Queue.EXCLUSIVE);
    }

    public void testIsNotExclusive() throws Exception
    {
        assertAttribute("exclusive", Boolean.FALSE, Queue.EXCLUSIVE);
    }

    public void testSetExclusive() throws Exception
    {
        testSetAttribute("exclusive", Queue.EXCLUSIVE, Boolean.FALSE , Boolean.TRUE);
    }

    /**********  Other attributes **********/

    public void testGetAlternateExchange()
    {
        Exchange mockAlternateExchange = mock(Exchange.class);
        when(mockAlternateExchange.getName()).thenReturn(QUEUE_ALTERNATE_EXCHANGE);

        when(_mockQueue.getAttribute(Queue.ALTERNATE_EXCHANGE)).thenReturn(mockAlternateExchange);

        assertEquals(QUEUE_ALTERNATE_EXCHANGE, _queueMBean.getAlternateExchange());
    }

    public void testGetAlternateExchangeWhenQueueHasNone()
    {
        when(_mockQueue.getAttribute(Queue.ALTERNATE_EXCHANGE)).thenReturn(null);

        assertNull(_queueMBean.getAlternateExchange());
    }

    public void testSetAlternateExchange() throws Exception
    {
        Exchange mockExchange1 = mock(Exchange.class);
        when(mockExchange1.getName()).thenReturn("exchange1");

        Exchange mockExchange2 = mock(Exchange.class);
        when(mockExchange2.getName()).thenReturn("exchange2");

        Exchange mockExchange3 = mock(Exchange.class);
        when(mockExchange3.getName()).thenReturn("exchange3");

        VirtualHost mockVirtualHost = mock(VirtualHost.class);
        when(mockVirtualHost.getExchanges()).thenReturn(Arrays.asList(new Exchange[] {mockExchange1, mockExchange2, mockExchange3}));
        when(_mockQueue.getParent(VirtualHost.class)).thenReturn(mockVirtualHost);

        _queueMBean.setAlternateExchange("exchange2");
        verify(_mockQueue).setAttribute(Queue.ALTERNATE_EXCHANGE, null, mockExchange2);
    }

    public void testSetAlternateExchangeWithUnknownExchangeName() throws Exception
    {
        Exchange mockExchange = mock(Exchange.class);
        when(mockExchange.getName()).thenReturn("exchange1");

        VirtualHost mockVirtualHost = mock(VirtualHost.class);
        when(mockVirtualHost.getExchanges()).thenReturn(Collections.singletonList(mockExchange));
        when(_mockQueue.getParent(VirtualHost.class)).thenReturn(mockVirtualHost);

        try
        {
            _queueMBean.setAlternateExchange("notknown");
            fail("Exception not thrown");
        }
        catch(OperationsException oe)
        {
            // PASS
        }
    }

    public void testRemoveAlternateExchange() throws Exception
    {
        _queueMBean.setAlternateExchange("");
        verify(_mockQueue).setAttribute(Queue.ALTERNATE_EXCHANGE, null, null);
    }

    /**********  Operations **********/

    /**********  Notifications **********/

    public void testNotificationListenerCalled() throws Exception
    {
        NotificationListener listener = mock(NotificationListener.class);
        _queueMBean.addNotificationListener(listener, null, null);

        NotificationCheck notification = mock(NotificationCheck.class);
        String notificationMsg = "Test notification message";

        _queueMBean.notifyClients(notification, _mockQueue, notificationMsg);
        verify(listener).handleNotification(isNotificationWithMessage(notificationMsg),
                                            isNull());
    }

    public void testAddRemoveNotificationListener() throws Exception
    {
        NotificationListener listener1 = mock(NotificationListener.class);
        _queueMBean.addNotificationListener(listener1, null, null);
        _queueMBean.removeNotificationListener(listener1);
    }

    public void testRemoveUnknownNotificationListener() throws Exception
    {
        NotificationListener listener1 = mock(NotificationListener.class);
        try
        {
            _queueMBean.removeNotificationListener(listener1);
            fail("Exception not thrown");
        }
        catch (ListenerNotFoundException e)
        {
            // PASS
        }
    }

    private Notification isNotificationWithMessage(final String expectedMessage)
    {
        return argThat( new ArgumentMatcher<Notification>()
        {
            @Override
            public boolean matches(Object argument)
            {
                Notification actual = (Notification) argument;
                return actual.getMessage().endsWith(expectedMessage);
            }
        });
    }

    private void assertStatistic(String jmxAttributeName, Object expectedValue, String underlyingAttributeName) throws Exception
    {
        when(_mockQueueStatistics.getStatistic(underlyingAttributeName)).thenReturn(expectedValue);
        MBeanTestUtils.assertMBeanAttribute(_queueMBean, jmxAttributeName, expectedValue);
    }

    private void assertAttribute(String jmxAttributeName, Object expectedValue, String underlyingAttributeName) throws Exception
    {
        when(_mockQueue.getAttribute(underlyingAttributeName)).thenReturn(expectedValue);
        MBeanTestUtils.assertMBeanAttribute(_queueMBean, jmxAttributeName, expectedValue);
    }

    private void testSetAttribute(String jmxAttributeName, String underlyingAttributeName, Object originalAttributeValue, Object newAttributeValue) throws Exception
    {
        when(_mockQueue.getAttribute(underlyingAttributeName)).thenReturn(originalAttributeValue);

        MBeanTestUtils.setMBeanAttribute(_queueMBean, jmxAttributeName, newAttributeValue);

        verify(_mockQueue).setAttribute(underlyingAttributeName, originalAttributeValue, newAttributeValue);
    }

    public void testViewMessageContent() throws Exception
    {
        viewMessageContentTestImpl(16L, 1000, 1000);
    }

    public void testViewMessageContentWithMissingPayload() throws Exception
    {
        viewMessageContentTestImpl(16L, 1000, 0);
    }

    private void viewMessageContentTestImpl(final long messageNumber,
                                       final int messageSize,
                                       final int messageContentSize) throws Exception
    {
        final byte[] content = new byte[messageContentSize];

        //mock message and queue entry to return a given message size, and have a given content
        final ServerMessage<?> serverMessage = mock(ServerMessage.class);
        when(serverMessage.getMessageNumber()).thenReturn(messageNumber);
        when(serverMessage.getSize()).thenReturn((long)messageSize);
        doAnswer(new Answer<Object>()
        {
            public Object answer(InvocationOnMock invocation)
            {
                Object[] args = invocation.getArguments();

                //verify the arg types / expected values
                assertEquals(2, args.length);
                assertTrue(args[0] instanceof ByteBuffer);
                assertTrue(args[1] instanceof Integer);

                ByteBuffer dest = (ByteBuffer) args[0];
                int offset = (Integer) args[1];
                assertEquals(0, offset);

                dest.put(content);
                return messageContentSize;
            }
        }).when(serverMessage).getContent(Matchers.any(ByteBuffer.class), Matchers.anyInt());

        final QueueEntry entry = mock(QueueEntry.class);
        when(entry.getMessage()).thenReturn(serverMessage);

        //mock the queue.visit() method to ensure we match the mock message
        doAnswer(new Answer<Object>()
        {
            public Object answer(InvocationOnMock invocation)
            {
                Object[] args = invocation.getArguments();
                GetMessageVisitor visitor = (GetMessageVisitor) args[0];
                visitor.visit(entry);
                return null;
            }
        }).when(_mockQueue).visit(Matchers.any(GetMessageVisitor.class));

        //now retrieve the content and verify its size
        CompositeDataSupport comp = (CompositeDataSupport) _queueMBean.viewMessageContent(messageNumber);
        assertNotNull(comp);
        byte[] data = (byte[]) comp.get(ManagedQueue.CONTENT);
        assertEquals(messageSize, data.length);
    }
}
