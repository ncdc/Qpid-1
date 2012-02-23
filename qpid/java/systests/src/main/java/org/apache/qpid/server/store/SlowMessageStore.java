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
import org.apache.log4j.Logger;

import org.apache.qpid.AMQStoreException;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.server.exchange.Exchange;
import org.apache.qpid.server.federation.Bridge;
import org.apache.qpid.server.federation.BrokerLink;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.message.EnqueableMessage;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.queue.AMQQueue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;

public class SlowMessageStore implements MessageStore, DurableConfigurationStore
{
    private static final Logger _logger = Logger.getLogger(SlowMessageStore.class);
    private static final String DELAYS = "delays";
    private HashMap<String, Long> _preDelays = new HashMap<String, Long>();
    private HashMap<String, Long> _postDelays = new HashMap<String, Long>();
    private long _defaultDelay = 0L;
    private MessageStore _realStore = new MemoryMessageStore();
    private DurableConfigurationStore _durableConfigurationStore = (MemoryMessageStore) _realStore;
    private static final String PRE = "pre";
    private static final String POST = "post";
    private String DEFAULT_DELAY = "default";

    // ***** MessageStore Interface.

    public void configureConfigStore(String name,
                          ConfigurationRecoveryHandler recoveryHandler,
                          Configuration config,
                          LogSubject logSubject) throws Exception
    {
        //To change body of implemented methods use File | Settings | File Templates.

        _logger.info("Starting SlowMessageStore on Virtualhost:" + name);
        Configuration delays = config.subset(DELAYS);

        configureDelays(delays);

        String messageStoreClass = config.getString("realStore");

        if (delays.containsKey(DEFAULT_DELAY))
        {
            _defaultDelay = delays.getLong(DEFAULT_DELAY);
        }

        if (messageStoreClass != null)
        {
            Class clazz = Class.forName(messageStoreClass);

            Object o = clazz.newInstance();

            if (!(o instanceof MessageStore))
            {
                throw new ClassCastException("Message store class must implement " + MessageStore.class + ". Class " + clazz +
                                             " does not.");
            }
            _realStore = (MessageStore) o;
            if(o instanceof DurableConfigurationStore)
            {
                _durableConfigurationStore = (DurableConfigurationStore)o;
            }
        }
        _durableConfigurationStore.configureConfigStore(name, recoveryHandler, config, logSubject);

    }

    private void configureDelays(Configuration config)
    {
        Iterator delays = config.getKeys();

        while (delays.hasNext())
        {
            String key = (String) delays.next();
            if (key.endsWith(PRE))
            {
                _preDelays.put(key.substring(0, key.length() - PRE.length() - 1), config.getLong(key));
            }
            else if (key.endsWith(POST))
            {
                _postDelays.put(key.substring(0, key.length() - POST.length() - 1), config.getLong(key));
            }
        }
    }

    private void doPostDelay(String method)
    {
        long delay = lookupDelay(_postDelays, method);
        doDelay(delay);
    }

    private void doPreDelay(String method)
    {
        long delay = lookupDelay(_preDelays, method);
        doDelay(delay);
    }

    private long lookupDelay(HashMap<String, Long> delays, String method)
    {
        Long delay = delays.get(method);
        return (delay == null) ? _defaultDelay : delay;
    }

    private void doDelay(long delay)
    {
        if (delay > 0)
        {
            long start = System.nanoTime();
            try
            {

                Thread.sleep(delay);
            }
            catch (InterruptedException e)
            {
                _logger.warn("Interrupted : " + e);
            }

            long slept = (System.nanoTime() - start) / 1000000;

            if (slept >= delay)
            {
                _logger.info("Done sleep for:" + slept+":"+delay);
            }
            else
            {
                _logger.info("Only sleep for:" + slept + " re-sleeping");
                doDelay(delay - slept);
            }
        }
    }


    public void configureMessageStore(String name,
                                      MessageStoreRecoveryHandler recoveryHandler,
                                      Configuration config,
                                      LogSubject logSubject) throws Exception
    {
        _realStore.configureMessageStore(name, recoveryHandler, config, logSubject);
    }

    public void close() throws Exception
    {
        doPreDelay("close");
        _realStore.close();
        doPostDelay("close");
    }

    public <M extends StorableMessageMetaData> StoredMessage<M> addMessage(M metaData)
    {
        return _realStore.addMessage(metaData);
    }


    public void createExchange(Exchange exchange) throws AMQStoreException
    {
        doPreDelay("createExchange");
        _durableConfigurationStore.createExchange(exchange);
        doPostDelay("createExchange");
    }

    public void removeExchange(Exchange exchange) throws AMQStoreException
    {
        doPreDelay("removeExchange");
        _durableConfigurationStore.removeExchange(exchange);
        doPostDelay("removeExchange");
    }

    public void bindQueue(Exchange exchange, AMQShortString routingKey, AMQQueue queue, FieldTable args) throws AMQStoreException
    {
        doPreDelay("bindQueue");
        _durableConfigurationStore.bindQueue(exchange, routingKey, queue, args);
        doPostDelay("bindQueue");
    }

    public void unbindQueue(Exchange exchange, AMQShortString routingKey, AMQQueue queue, FieldTable args) throws AMQStoreException
    {
        doPreDelay("unbindQueue");
        _durableConfigurationStore.unbindQueue(exchange, routingKey, queue, args);
        doPostDelay("unbindQueue");
    }

    public void createQueue(AMQQueue queue) throws AMQStoreException
    {
        createQueue(queue, null);
    }

    public void createQueue(AMQQueue queue, FieldTable arguments) throws AMQStoreException
    {
        doPreDelay("createQueue");
        _durableConfigurationStore.createQueue(queue, arguments);
        doPostDelay("createQueue");
    }

    public void removeQueue(AMQQueue queue) throws AMQStoreException
    {
        doPreDelay("removeQueue");
        _durableConfigurationStore.removeQueue(queue);
        doPostDelay("removeQueue");
    }

    public void configureTransactionLog(String name,
                                        TransactionLogRecoveryHandler recoveryHandler,
                                        Configuration storeConfiguration, LogSubject logSubject)
            throws Exception
    {
        _realStore.configureTransactionLog(name, recoveryHandler, storeConfiguration, logSubject);
    }

    public Transaction newTransaction()
    {
        doPreDelay("beginTran");
        Transaction txn = new SlowTransaction(_realStore.newTransaction());
        doPostDelay("beginTran");
        return txn;
    }


    public boolean isPersistent()
    {
        return _realStore.isPersistent();
    }

    public void storeMessageHeader(Long messageNumber, ServerMessage message)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void storeContent(Long messageNumber, long offset, ByteBuffer body)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public ServerMessage getMessage(Long messageNumber)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private class SlowTransaction implements Transaction
    {
        private final Transaction _underlying;

        private SlowTransaction(Transaction underlying)
        {
            _underlying = underlying;
        }

        public void enqueueMessage(TransactionLogResource queue, EnqueableMessage message)
                throws AMQStoreException
        {
            doPreDelay("enqueueMessage");
            _underlying.enqueueMessage(queue, message);
            doPostDelay("enqueueMessage");
        }

        public void dequeueMessage(TransactionLogResource queue, EnqueableMessage message)
                throws AMQStoreException
        {
            doPreDelay("dequeueMessage");
            _underlying.dequeueMessage(queue, message);
            doPostDelay("dequeueMessage");
        }

        public void commitTran()
                throws AMQStoreException
        {
            doPreDelay("commitTran");
            _underlying.commitTran();
            doPostDelay("commitTran");
        }

        public StoreFuture commitTranAsync()
                throws AMQStoreException
        {
            doPreDelay("commitTran");
            StoreFuture future = _underlying.commitTranAsync();
            doPostDelay("commitTran");
            return future;
        }

        public void abortTran()
                throws AMQStoreException
        {
            doPreDelay("abortTran");
            _underlying.abortTran();
            doPostDelay("abortTran");
        }

        public void removeXid(long format, byte[] globalId, byte[] branchId) throws AMQStoreException
        {
            _underlying.removeXid(format, globalId, branchId);
        }

        public void recordXid(long format, byte[] globalId, byte[] branchId, Record[] enqueues, Record[] dequeues)
                throws AMQStoreException
        {
            _underlying.recordXid(format, globalId, branchId, enqueues, dequeues);
        }
    }

    public void updateQueue(AMQQueue queue) throws AMQStoreException
    {
        doPreDelay("updateQueue");
        _durableConfigurationStore.updateQueue(queue);
        doPostDelay("updateQueue");
    }


    public void createBrokerLink(final BrokerLink link) throws AMQStoreException
    {
        doPreDelay("createBrokerLink");
        _durableConfigurationStore.createBrokerLink(link);
        doPostDelay("createBrokerLink");
    }

    public void deleteBrokerLink(final BrokerLink link) throws AMQStoreException
    {
        doPreDelay("deleteBrokerLink");
        _durableConfigurationStore.deleteBrokerLink(link);
        doPostDelay("deleteBrokerLink");
    }

    public void createBridge(final Bridge bridge) throws AMQStoreException
    {
        doPreDelay("createBridge");
        _durableConfigurationStore.createBridge(bridge);
        doPostDelay("createBridge");
    }

    public void deleteBridge(final Bridge bridge) throws AMQStoreException
    {
        doPreDelay("deleteBridge");
        _durableConfigurationStore.deleteBridge(bridge);
        doPostDelay("deleteBridge");
    }
}
