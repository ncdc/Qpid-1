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
package org.apache.qpid.server.store;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.qpid.server.util.InternalBrokerBaseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.AMQException;
import org.apache.qpid.common.AMQPFilterTypes;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.BasicContentHeaderProperties;
import org.apache.qpid.framing.ContentHeaderBody;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.framing.abstraction.MessagePublishInfo;
import org.apache.qpid.framing.amqp_8_0.BasicConsumeBodyImpl;
import org.apache.qpid.server.binding.Binding;
import org.apache.qpid.server.configuration.VirtualHostConfiguration;
import org.apache.qpid.server.exchange.DirectExchange;
import org.apache.qpid.server.exchange.Exchange;
import org.apache.qpid.server.exchange.ExchangeRegistry;
import org.apache.qpid.server.exchange.ExchangeType;
import org.apache.qpid.server.exchange.TopicExchange;
import org.apache.qpid.server.message.AMQMessage;
import org.apache.qpid.server.message.MessageMetaData;
import org.apache.qpid.server.queue.AMQPriorityQueue;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.queue.AMQQueueFactory;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.queue.IncomingMessage;
import org.apache.qpid.server.queue.QueueRegistry;
import org.apache.qpid.server.queue.SimpleAMQQueue;
import org.apache.qpid.server.registry.ApplicationRegistry;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.virtualhost.VirtualHost;
import org.apache.qpid.util.FileUtils;

import java.io.File;
import java.util.List;

/**
 * This tests the MessageStores by using the available interfaces.
 *
 * This test validates that Exchanges, Queues, Bindings and Messages are persisted and
 * recovered correctly.
 */
public class MessageStoreTest extends InternalBrokerBaseCase
{

    private static final int DEFAULT_PRIORTY_LEVEL = 5;
    private static final Logger _logger = LoggerFactory.getLogger(MessageStoreTest.class);

    public void testMemoryMessageStore() throws Exception
    {

        PropertiesConfiguration config = new PropertiesConfiguration();

        config.addProperty("store.class", "org.apache.qpid.server.store.MemoryMessageStore");

        runTestWithStore(config);
    }

    public void testDerbyMessageStore() throws Exception
    {
        PropertiesConfiguration config = new PropertiesConfiguration();

        config.addProperty("store.environment-path", "derbyDB_MST");
        config.addProperty("store.class", "org.apache.qpid.server.store.DerbyMessageStore");

        runTestWithStore(config);
    }

    private void reload(Configuration configuration)
    {
        if (_virtualHost != null)
        {
            try
            {
                _virtualHost.close();
                _virtualHost.getApplicationRegistry().
                        getVirtualHostRegistry().unregisterVirtualHost(_virtualHost);
            }
            catch (Exception e)
            {
                fail(e.getMessage());
                
            }
        }

        try
        {
            _virtualHost = ApplicationRegistry.getInstance().createVirtualHost(new VirtualHostConfiguration(getClass().getName(), configuration));
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    AMQShortString nonDurableExchangeName = new AMQShortString("MST-NonDurableDirectExchange");
    AMQShortString directExchangeName = new AMQShortString("MST-DirectExchange");
    AMQShortString topicExchangeName = new AMQShortString("MST-TopicExchange");
    AMQShortString queueOwner = new AMQShortString("MST");

    AMQShortString durablePriorityTopicQueueName = new AMQShortString("MST-PriorityTopicQueue-Durable");
    AMQShortString durableTopicQueueName = new AMQShortString("MST-TopicQueue-Durable");
    AMQShortString priorityTopicQueueName = new AMQShortString("MST-PriorityTopicQueue");
    AMQShortString topicQueueName = new AMQShortString("MST-TopicQueue");

    AMQShortString durableExclusiveQueueName = new AMQShortString("MST-Queue-Durable-Exclusive");
    AMQShortString durablePriorityQueueName = new AMQShortString("MST-PriorityQueue-Durable");
    AMQShortString durableQueueName = new AMQShortString("MST-Queue-Durable");
    AMQShortString priorityQueueName = new AMQShortString("MST-PriorityQueue");
    AMQShortString queueName = new AMQShortString("MST-Queue");

    AMQShortString directRouting = new AMQShortString("MST-direct");
    AMQShortString topicRouting = new AMQShortString("MST-topic");

    
    protected void runTestWithStore(Configuration configuration) throws AMQException
    {
        //Ensure Environment Path is empty
        cleanup(configuration);

        //Load the Virtualhost with the required MessageStore
        reload(configuration);

        MessageStore messageStore = _virtualHost.getMessageStore();

        createAllQueues();
        createAllTopicQueues();

        //Register Non-Durable DirectExchange
        Exchange nonDurableExchange = createExchange(DirectExchange.TYPE, nonDurableExchangeName, false);
        bindAllQueuesToExchange(nonDurableExchange, directRouting);

        //Register DirectExchange
        Exchange directExchange = createExchange(DirectExchange.TYPE, directExchangeName, true);
        bindAllQueuesToExchange(directExchange, directRouting);

        //Register TopicExchange
        Exchange topicExchange = createExchange(TopicExchange.TYPE, topicExchangeName, true);
        bindAllTopicQueuesToExchange(topicExchange, topicRouting);

        //Send Message To NonDurable direct Exchange = persistent
        sendMessageOnExchange(nonDurableExchange, directRouting, true);
        // and non-persistent
        sendMessageOnExchange(nonDurableExchange, directRouting, false);

        //Send Message To direct Exchange = persistent
        sendMessageOnExchange(directExchange, directRouting, true);
        // and non-persistent
        sendMessageOnExchange(directExchange, directRouting, false);

        //Send Message To topic Exchange = persistent
        sendMessageOnExchange(topicExchange, topicRouting, true);
        // and non-persistent
        sendMessageOnExchange(topicExchange, topicRouting, false);

        //Ensure all the Queues have four messages (one transient, one persistent) x 2 exchange routings
        validateMessageOnQueues(4, true);
        //Ensure all the topics have two messages (one transient, one persistent)
        validateMessageOnTopics(2, true);

        assertEquals("Not all queues correctly registered", 9, _virtualHost.getQueueRegistry().getQueues().size());

        if (!messageStore.isPersistent())
        {
            _logger.warn("Unable to test Persistent capabilities of messages store(" + messageStore.getClass() + ") as it is not capable of peristence.");
            return;
        }

        //Reload the Virtualhost to test persistence
        _logger.info("Reloading Virtualhost");

        VirtualHost original = _virtualHost;

        reload(configuration);

        assertTrue("Virtualhost has not been reloaded", original != _virtualHost);

        validateExchanges();

        //Validate Durable Queues still have the persistent message
        validateMessageOnQueues(2, false);
        //Validate Durable Queues still have the persistent message
        validateMessageOnTopics(1, false);

        //Validate Properties of Binding
        validateBindingProperties();

        //Validate Properties of Queues
        validateQueueProperties();
        validateQueueExclusivityProperty(true);

        //Validate Non-Durable Queues are gone.
        assertNull("Non-Durable queue still registered:" + priorityQueueName, _virtualHost.getQueueRegistry().getQueue(priorityQueueName));
        assertNull("Non-Durable queue still registered:" + queueName, _virtualHost.getQueueRegistry().getQueue(queueName));
        assertNull("Non-Durable queue still registered:" + priorityTopicQueueName, _virtualHost.getQueueRegistry().getQueue(priorityTopicQueueName));
        assertNull("Non-Durable queue still registered:" + topicQueueName, _virtualHost.getQueueRegistry().getQueue(topicQueueName));

        assertEquals("Not all queues correctly registered", 5, _virtualHost.getQueueRegistry().getQueues().size());
        
        //Try updating the queue exclusivity and verify it is persisted and recovered correctly
        setQueueExclusivity(false);
        
        //Reload the Virtualhost to test update to queue exclusivity
        _logger.info("Reloading Virtualhost");
        original = _virtualHost;
        reload(configuration);
        
        assertTrue("Virtualhost has not been reloaded", original != _virtualHost);

        //verify the change was persisted
        validateQueueExclusivityProperty(false);
    }

    private void validateExchanges()
    {
        ExchangeRegistry registry = _virtualHost.getExchangeRegistry();

        assertTrue(directExchangeName + " exchange NOT reloaded after failover",
                   registry.getExchangeNames().contains(directExchangeName));
        assertTrue(topicExchangeName + " exchange NOT reloaded after failover",
                   registry.getExchangeNames().contains(topicExchangeName));
        assertTrue(nonDurableExchangeName + " exchange reloaded after failover",
                   !registry.getExchangeNames().contains(nonDurableExchangeName));

        // There are 5 required exchanges + qpid.management + our 2 durable exchanges
        assertEquals("Incorrect number of exchanges available", 6 + 2, registry.getExchangeNames().size());
    }

    /** Validates that the Durable queues */
    private void validateBindingProperties()
    {
        QueueRegistry queueRegistry = _virtualHost.getQueueRegistry();

        validateBindingProperties(queueRegistry.getQueue(durablePriorityQueueName).getBindings(), false);
        validateBindingProperties(queueRegistry.getQueue(durablePriorityTopicQueueName).getBindings(), true);
        validateBindingProperties(queueRegistry.getQueue(durableQueueName).getBindings(), false);
        validateBindingProperties(queueRegistry.getQueue(durableTopicQueueName).getBindings(), true);
    }

    /**
     * Validate that each queue is bound once.
     *
     * @param bindings     the set of bindings to validate
     * @param useSelectors if set validate that the binding has a JMS_SELECTOR argument
     */
    private void validateBindingProperties(List<Binding> bindings, boolean useSelectors)
    {
        assertEquals("Each queue should only be bound once.", 1, bindings.size());

        Binding binding = bindings.get(0);

        if (useSelectors)
        {
            assertTrue("Binding does not contain a Selector argument.",
                       binding.getArguments().containsKey(AMQPFilterTypes.JMS_SELECTOR.getValue()));
        }
    }
    
    private void setQueueExclusivity(boolean exclusive) throws AMQException
    {
        QueueRegistry queueRegistry = _virtualHost.getQueueRegistry();

        AMQQueue queue = queueRegistry.getQueue(durableExclusiveQueueName);
        
        queue.setExclusive(exclusive);
    }

    private void validateQueueExclusivityProperty(boolean expected)
    {
        QueueRegistry queueRegistry = _virtualHost.getQueueRegistry();

        AMQQueue queue = queueRegistry.getQueue(durableExclusiveQueueName);
        
        assertEquals("Queue exclusivity was incorrect", queue.isExclusive(), expected);
    }

    
    private void validateQueueProperties()
    {
        QueueRegistry queueRegistry = _virtualHost.getQueueRegistry();

        validateQueueProperties(queueRegistry.getQueue(durablePriorityQueueName), true);
        validateQueueProperties(queueRegistry.getQueue(durablePriorityTopicQueueName), true);
        validateQueueProperties(queueRegistry.getQueue(durableQueueName), false);
        validateQueueProperties(queueRegistry.getQueue(durableTopicQueueName), false);
    }

    private void validateQueueProperties(AMQQueue queue, boolean usePriority)
    {
        if (usePriority)
        {
            assertEquals("Queue is no longer a Priority Queue", AMQPriorityQueue.class, queue.getClass());
            assertEquals("Priority Queue does not have set priorities", DEFAULT_PRIORTY_LEVEL, ((AMQPriorityQueue) queue).getPriorities());
        }
        else
        {
            assertEquals("Queue is no longer a Priority Queue", SimpleAMQQueue.class, queue.getClass());
        }
    }

    /**
     * Delete the Store Environment path
     *
     * @param configuration The configuration that contains the store environment path.
     */
    private void cleanup(Configuration configuration)
    {

        String environment = configuration.getString("store.environment-path");

        if (environment != null)
        {
            File environmentPath = new File(environment);
            
            if (environmentPath.exists())
            {
                FileUtils.delete(environmentPath, true);
            }
        }
    }

    private void sendMessageOnExchange(Exchange directExchange, AMQShortString routingKey, boolean deliveryMode)
    {
        //Set MessagePersustebce
        BasicContentHeaderProperties properties = new BasicContentHeaderProperties();
        properties.setDeliveryMode(deliveryMode ? Integer.valueOf(2).byteValue() : Integer.valueOf(1).byteValue());
        FieldTable headers = properties.getHeaders();
        headers.setString("Test", "MST");
        properties.setHeaders(headers);

        MessagePublishInfo messageInfo = new TestMessagePublishInfo(directExchange, false, false, routingKey);

        final IncomingMessage currentMessage;


        currentMessage = new IncomingMessage(messageInfo);

        currentMessage.setExchange(directExchange);

        ContentHeaderBody headerBody = new ContentHeaderBody();
        headerBody.classId = BasicConsumeBodyImpl.CLASS_ID;
        headerBody.bodySize = 0;

        headerBody.properties = properties;

        try
        {
            currentMessage.setContentHeaderBody(headerBody);
        }
        catch (AMQException e)
        {
            fail(e.getMessage());
        }

        currentMessage.setExpiration();

        MessageMetaData mmd = currentMessage.headersReceived();
        currentMessage.setStoredMessage(_virtualHost.getMessageStore().addMessage(mmd));
        currentMessage.getStoredMessage().flushToStore();
        currentMessage.route();


        // check and deliver if header says body length is zero
        if (currentMessage.allContentReceived())
        {
            // TODO Deliver to queues
            ServerTransaction trans = new AutoCommitTransaction(_virtualHost.getMessageStore());
            final List<? extends BaseQueue> destinationQueues = currentMessage.getDestinationQueues();
            trans.enqueue(currentMessage.getDestinationQueues(), currentMessage, new ServerTransaction.Action() {
                public void postCommit()
                {
                    try
                    {
                        AMQMessage message = new AMQMessage(currentMessage.getStoredMessage());

                        for(BaseQueue queue : destinationQueues)
                        {
                            queue.enqueue(message);
                        }
                    }
                    catch (AMQException e)
                    {
                        e.printStackTrace();
                    }
                }

                public void onRollback()
                {
                    //To change body of implemented methods use File | Settings | File Templates.
                }
            });
        }
    }

    private void createAllQueues()
    {
        //Register Durable Priority Queue
        createQueue(durablePriorityQueueName, true, true, false);

        //Register Durable Simple Queue
        createQueue(durableQueueName, false, true, false);
        
        //Register Durable Exclusive Simple Queue
        createQueue(durableExclusiveQueueName, false, true, true);
        
        //Register NON-Durable Priority Queue
        createQueue(priorityQueueName, true, false, false);

        //Register NON-Durable Simple Queue
        createQueue(queueName, false, false, false);
    }

    private void createAllTopicQueues()
    {
        //Register Durable Priority Queue
        createQueue(durablePriorityTopicQueueName, true, true, false);

        //Register Durable Simple Queue
        createQueue(durableTopicQueueName, false, true, false);

        //Register NON-Durable Priority Queue
        createQueue(priorityTopicQueueName, true, false, false);

        //Register NON-Durable Simple Queue
        createQueue(topicQueueName, false, false, false);
    }

    private Exchange createExchange(ExchangeType type, AMQShortString name, boolean durable)
    {
        Exchange exchange = null;

        try
        {
            exchange = type.newInstance(_virtualHost, name, durable, 0, false);
        }
        catch (AMQException e)
        {
            fail(e.getMessage());
        }

        try
        {
            _virtualHost.getExchangeRegistry().registerExchange(exchange);
            if (durable)
            {
                _virtualHost.getMessageStore().createExchange(exchange);
            }
        }
        catch (AMQException e)
        {
            fail(e.getMessage());
        }
        return exchange;
    }

    private void createQueue(AMQShortString queueName, boolean usePriority, boolean durable, boolean exclusive)
    {

        FieldTable queueArguments = null;

        if (usePriority)
        {
            queueArguments = new FieldTable();
            queueArguments.put(AMQQueueFactory.X_QPID_PRIORITIES, DEFAULT_PRIORTY_LEVEL);
        }

        AMQQueue queue = null;

        //Ideally we would be able to use the QueueDeclareHandler here.
        try
        {
            queue = AMQQueueFactory.createAMQQueueImpl(queueName, durable, queueOwner, false, exclusive,
                                                       _virtualHost, queueArguments);

            validateQueueProperties(queue, usePriority);

            if (queue.isDurable() && !queue.isAutoDelete())
            {
                _virtualHost.getMessageStore().createQueue(queue, queueArguments);
            }
        }
        catch (AMQException e)
        {
            fail(e.getMessage());
        }

        _virtualHost.getQueueRegistry().registerQueue(queue);

    }

    private void bindAllQueuesToExchange(Exchange exchange, AMQShortString routingKey)
    {
        FieldTable queueArguments = new FieldTable();
        queueArguments.put(AMQQueueFactory.X_QPID_PRIORITIES, DEFAULT_PRIORTY_LEVEL);

        QueueRegistry queueRegistry = _virtualHost.getQueueRegistry();

        bindQueueToExchange(exchange, routingKey, queueRegistry.getQueue(durablePriorityQueueName), false, queueArguments);
        bindQueueToExchange(exchange, routingKey, queueRegistry.getQueue(durableQueueName), false, null);
        bindQueueToExchange(exchange, routingKey, queueRegistry.getQueue(priorityQueueName), false, queueArguments);
        bindQueueToExchange(exchange, routingKey, queueRegistry.getQueue(queueName), false, null);
    }

    private void bindAllTopicQueuesToExchange(Exchange exchange, AMQShortString routingKey)
    {
        FieldTable queueArguments = new FieldTable();
        queueArguments.put(AMQQueueFactory.X_QPID_PRIORITIES, DEFAULT_PRIORTY_LEVEL);

        QueueRegistry queueRegistry = _virtualHost.getQueueRegistry();

        bindQueueToExchange(exchange, routingKey, queueRegistry.getQueue(durablePriorityTopicQueueName), true, queueArguments);
        bindQueueToExchange(exchange, routingKey, queueRegistry.getQueue(durableTopicQueueName), true, null);
        bindQueueToExchange(exchange, routingKey, queueRegistry.getQueue(priorityTopicQueueName), true, queueArguments);
        bindQueueToExchange(exchange, routingKey, queueRegistry.getQueue(topicQueueName), true, null);
    }


    protected void bindQueueToExchange(Exchange exchange, AMQShortString routingKey, AMQQueue queue, boolean useSelector, FieldTable queueArguments)
    {


        FieldTable bindArguments = null;

        if (useSelector)
        {
            bindArguments = new FieldTable();
            bindArguments.put(AMQPFilterTypes.JMS_SELECTOR.getValue(), "Test = 'MST'");
        }

        try
        {
            _virtualHost.getBindingFactory().addBinding(String.valueOf(routingKey), queue, exchange, FieldTable.convertToMap(bindArguments));
        }
        catch (Exception e)
        {
            fail(e.getMessage());
        }
    }

    private void validateMessage(long messageCount, boolean allQueues)
    {
        validateMessageOnTopics(messageCount, allQueues);
        validateMessageOnQueues(messageCount, allQueues);
    }

    private void validateMessageOnTopics(long messageCount, boolean allQueues)
    {
        validateMessageOnQueue(durablePriorityTopicQueueName, messageCount);
        validateMessageOnQueue(durableTopicQueueName, messageCount);

        if (allQueues)
        {
            validateMessageOnQueue(priorityTopicQueueName, messageCount);
            validateMessageOnQueue(topicQueueName, messageCount);
        }
    }

    private void validateMessageOnQueues(long messageCount, boolean allQueues)
    {
        validateMessageOnQueue(durablePriorityQueueName, messageCount);
        validateMessageOnQueue(durableQueueName, messageCount);

        if (allQueues)
        {
            validateMessageOnQueue(priorityQueueName, messageCount);
            validateMessageOnQueue(queueName, messageCount);
        }
    }

    private void validateMessageOnQueue(AMQShortString queueName, long messageCount)
    {
        AMQQueue queue = _virtualHost.getQueueRegistry().getQueue(queueName);

        assertNotNull("Queue(" + queueName + ") not correctly registered:", queue);

        assertEquals("Incorrect Message count on queue:" + queueName, messageCount, queue.getMessageCount());
    }

    private class TestMessagePublishInfo implements MessagePublishInfo
    {

        Exchange _exchange;
        boolean _immediate;
        boolean _mandatory;
        AMQShortString _routingKey;

        TestMessagePublishInfo(Exchange exchange, boolean immediate, boolean mandatory, AMQShortString routingKey)
        {
            _exchange = exchange;
            _immediate = immediate;
            _mandatory = mandatory;
            _routingKey = routingKey;
        }

        public AMQShortString getExchange()
        {
            return _exchange.getNameShortString();
        }

        public void setExchange(AMQShortString exchange)
        {
            //no-op
        }

        public boolean isImmediate()
        {
            return _immediate;
        }

        public boolean isMandatory()
        {
            return _mandatory;
        }

        public AMQShortString getRoutingKey()
        {
            return _routingKey;
        }
    }
}