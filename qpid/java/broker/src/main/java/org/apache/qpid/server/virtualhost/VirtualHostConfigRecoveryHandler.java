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
package org.apache.qpid.server.virtualhost;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.qpid.AMQException;
import org.apache.qpid.AMQStoreException;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.server.binding.BindingFactory;
import org.apache.qpid.server.exchange.Exchange;
import org.apache.qpid.server.logging.actors.CurrentActor;
import org.apache.qpid.server.logging.messages.TransactionLogMessages;
import org.apache.qpid.server.logging.subjects.MessageStoreLogSubject;
import org.apache.qpid.server.message.AMQMessage;
import org.apache.qpid.server.message.AbstractServerMessageImpl;
import org.apache.qpid.server.message.EnqueableMessage;
import org.apache.qpid.server.message.MessageTransferMessage;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.queue.AMQQueueFactory;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.store.ConfigurationRecoveryHandler;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.MessageStoreRecoveryHandler;
import org.apache.qpid.server.store.StoredMessage;
import org.apache.qpid.server.store.Transaction;
import org.apache.qpid.server.store.TransactionLogRecoveryHandler;
import org.apache.qpid.server.store.TransactionLogResource;
import org.apache.qpid.server.txn.DtxBranch;
import org.apache.qpid.server.txn.DtxRegistry;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.transport.Xid;
import org.apache.qpid.transport.util.Functions;
import org.apache.qpid.util.ByteBufferInputStream;

public class VirtualHostConfigRecoveryHandler implements ConfigurationRecoveryHandler,
                                                        ConfigurationRecoveryHandler.QueueRecoveryHandler,
                                                        ConfigurationRecoveryHandler.ExchangeRecoveryHandler,
                                                        ConfigurationRecoveryHandler.BindingRecoveryHandler,
                                                        MessageStoreRecoveryHandler,
                                                        MessageStoreRecoveryHandler.StoredMessageRecoveryHandler,
                                                        TransactionLogRecoveryHandler,
                                                        TransactionLogRecoveryHandler.QueueEntryRecoveryHandler,
                                                        TransactionLogRecoveryHandler.DtxRecordRecoveryHandler
{
    private static final Logger _logger = Logger.getLogger(VirtualHostConfigRecoveryHandler.class);

    private final VirtualHost _virtualHost;

    private final Map<String, Integer> _queueRecoveries = new TreeMap<String, Integer>();
    private final Map<Long, AbstractServerMessageImpl> _recoveredMessages = new HashMap<Long, AbstractServerMessageImpl>();
    private final Map<Long, StoredMessage> _unusedMessages = new HashMap<Long, StoredMessage>();

    private MessageStoreLogSubject _logSubject;
    private MessageStore _store;

    public VirtualHostConfigRecoveryHandler(VirtualHost virtualHost)
    {
        _virtualHost = virtualHost;
    }

    public VirtualHostConfigRecoveryHandler begin(MessageStore store)
    {
        _logSubject = new MessageStoreLogSubject(_virtualHost,store.getClass().getSimpleName());
        _store = store;
        CurrentActor.get().message(_logSubject, TransactionLogMessages.RECOVERY_START(null, false));

        return this;
    }

    public void queue(UUID id, String queueName, String owner, boolean exclusive, FieldTable arguments, UUID alternateExchangeId)
    {
        try
        {
            AMQQueue q = _virtualHost.getQueueRegistry().getQueue(queueName);
    
            if (q == null)
            {
                q = AMQQueueFactory.createAMQQueueImpl(id, queueName, true, owner, false, exclusive, _virtualHost,
                                                       FieldTable.convertToMap(arguments));
                _virtualHost.getQueueRegistry().registerQueue(q);

                if (alternateExchangeId != null)
                {
                    Exchange altExchange = _virtualHost.getExchangeRegistry().getExchange(alternateExchangeId);
                    if (altExchange == null)
                    {
                        _logger.error("Unknown exchange id " + alternateExchangeId + ", cannot set alternate exchange on queue with id " + id);
                        return;
                    }
                    q.setAlternateExchange(altExchange);
                }
            }
    
            CurrentActor.get().message(_logSubject, TransactionLogMessages.RECOVERY_START(queueName, true));
    
            //Record that we have a queue for recovery
            _queueRecoveries.put(queueName, 0);
        }
        catch (AMQException e)
        {
            throw new RuntimeException("Error recovering queue uuid " + id + " name " + queueName, e);
        }
    }

    @Override
    public BindingRecoveryHandler completeQueueRecovery()
    {
        return this;
    }

    public void exchange(UUID id, String exchangeName, String type, boolean autoDelete)
    {
        try
        {
            Exchange exchange;
            AMQShortString exchangeNameSS = new AMQShortString(exchangeName);
            exchange = _virtualHost.getExchangeRegistry().getExchange(exchangeNameSS);
            if (exchange == null)
            {
                exchange = _virtualHost.getExchangeFactory().createExchange(id, exchangeNameSS, new AMQShortString(type), true, autoDelete, 0);
                _virtualHost.getExchangeRegistry().registerExchange(exchange);
            }
        }
        catch (AMQException e)
        {
            throw new RuntimeException("Error recovering exchange uuid " + id + " name " + exchangeName, e);
        }
    }

    public QueueRecoveryHandler completeExchangeRecovery()
    {
        return this;
    }

    public StoredMessageRecoveryHandler begin()
    {
        return this;
    }

    public void message(StoredMessage message)
    {
        AbstractServerMessageImpl serverMessage;
        switch(message.getMetaData().getType())
        {
            case META_DATA_0_8:
                serverMessage = new AMQMessage(message);
                break;
            case META_DATA_0_10:
                serverMessage = new MessageTransferMessage(message, null);
                break;
            default:
                throw new RuntimeException("Unknown message type retrieved from store " + message.getMetaData().getClass());
        }

        _recoveredMessages.put(message.getMessageNumber(), serverMessage);
        _unusedMessages.put(message.getMessageNumber(), message);
    }

    public void completeMessageRecovery()
    {
    }

    public void dtxRecord(long format, byte[] globalId, byte[] branchId,
                          Transaction.Record[] enqueues,
                          Transaction.Record[] dequeues)
    {
        Xid id = new Xid(format, globalId, branchId);
        DtxRegistry dtxRegistry = _virtualHost.getDtxRegistry();
        DtxBranch branch = dtxRegistry.getBranch(id);
        if(branch == null)
        {
            branch = new DtxBranch(id, _store, _virtualHost);
            dtxRegistry.registerBranch(branch);
        }
        for(Transaction.Record record : enqueues)
        {
            final AMQQueue queue = _virtualHost.getQueueRegistry().getQueue(record.getQueue().getId());
            if(queue != null)
            {
                final long messageId = record.getMessage().getMessageNumber();
                final AbstractServerMessageImpl message = _recoveredMessages.get(messageId);
                _unusedMessages.remove(messageId);

                if(message != null)
                {
                    message.incrementReference();

                    branch.enqueue(queue,message);

                    branch.addPostTransactionAcion(new ServerTransaction.Action()
                    {

                        public void postCommit()
                        {
                            try
                            {

                                queue.enqueue(message, true, null);
                                message.decrementReference();
                            }
                            catch (AMQException e)
                            {
                                _logger.error("Unable to enqueue message " + message.getMessageNumber() + " into " +
                                              "queue " + queue.getName() + " (from XA transaction)", e);
                                throw new RuntimeException(e);
                            }
                        }

                        public void onRollback()
                        {
                            message.decrementReference();
                        }
                    });
                }
                else
                {
                    StringBuilder xidString = xidAsString(id);
                    CurrentActor.get().message(_logSubject,
                                               TransactionLogMessages.XA_INCOMPLETE_MESSAGE(xidString.toString(),
                                                                                            Long.toString(messageId)));
                    
                }

            }
            else
            {
                StringBuilder xidString = xidAsString(id);
                CurrentActor.get().message(_logSubject,
                                           TransactionLogMessages.XA_INCOMPLETE_QUEUE(xidString.toString(),
                                                                                      record.getQueue().getId().toString()));

            }
        }
        for(Transaction.Record record : dequeues)
        {
            final AMQQueue queue = _virtualHost.getQueueRegistry().getQueue(record.getQueue().getId());
            if(queue != null)
            {
                final long messageId = record.getMessage().getMessageNumber();
                final AbstractServerMessageImpl message = _recoveredMessages.get(messageId);
                _unusedMessages.remove(messageId);

                if(message != null)
                {
                    final QueueEntry entry = queue.getMessageOnTheQueue(messageId);
                    
                    entry.acquire();
                    
                    branch.dequeue(queue, message);

                    branch.addPostTransactionAcion(new ServerTransaction.Action()
                    {

                        public void postCommit()
                        {
                            entry.discard();
                        }

                        public void onRollback()
                        {
                            entry.release();
                        }
                    });
                }
                else
                {
                    StringBuilder xidString = xidAsString(id);
                    CurrentActor.get().message(_logSubject,
                                               TransactionLogMessages.XA_INCOMPLETE_MESSAGE(xidString.toString(),
                                                                                            Long.toString(messageId)));

                }

            }
            else
            {
                StringBuilder xidString = xidAsString(id);
                CurrentActor.get().message(_logSubject,
                                           TransactionLogMessages.XA_INCOMPLETE_QUEUE(xidString.toString(),
                                                                                      record.getQueue().getId().toString()));
            }

        }

        try
        {
            branch.setState(DtxBranch.State.PREPARED);
            branch.prePrepareTransaction();
        }
        catch (AMQStoreException e)
        {
            _logger.error("Unexpected database exception when attempting to prepare a recovered XA transaction " +
                          xidAsString(id), e);
            throw new RuntimeException(e);
        }
    }

    private static StringBuilder xidAsString(Xid id)
    {
        return new StringBuilder("(")
                    .append(id.getFormat())
                    .append(',')
                    .append(Functions.str(id.getGlobalId()))
                    .append(',')
                    .append(Functions.str(id.getBranchId()))
                    .append(')');
    }

    public void completeDtxRecordRecovery()
    {
        for(StoredMessage m : _unusedMessages.values())
        {
            _logger.warn("Message id " + m.getMessageNumber() + " in store, but not in any queue - removing....");
            m.remove();
        }
        CurrentActor.get().message(_logSubject, TransactionLogMessages.RECOVERY_COMPLETE(null, false));
    }

    @Override
    public void binding(UUID bindingId, UUID exchangeId, UUID queueId, String bindingKey, ByteBuffer buf)
    {
        try
        {
            Exchange exchange = _virtualHost.getExchangeRegistry().getExchange(exchangeId);
            if (exchange == null)
            {
                _logger.error("Unknown exchange id " + exchangeId + ", cannot bind queue with id " + queueId);
                return;
            }

            AMQQueue queue = _virtualHost.getQueueRegistry().getQueue(queueId);
            if (queue == null)
            {
                _logger.error("Unknown queue id " + queueId + ", cannot be bound to exchange: " + exchange.getName());
            }
            else
            {
                FieldTable argumentsFT = null;
                if(buf != null)
                {
                    try
                    {
                        argumentsFT = new FieldTable(new DataInputStream(new ByteBufferInputStream(buf)),buf.limit());
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException("IOException should not be thrown here", e);
                    }
                }

                BindingFactory bf = _virtualHost.getBindingFactory();

                Map<String, Object> argumentMap = FieldTable.convertToMap(argumentsFT);

                if(bf.getBinding(bindingKey, queue, exchange, argumentMap) == null)
                {

                    _logger.info("Restoring binding: (Exchange: " + exchange.getNameShortString() + ", Queue: " + queue.getName()
                        + ", Routing Key: " + bindingKey + ", Arguments: " + argumentsFT + ")");

                    bf.restoreBinding(bindingId, bindingKey, queue, exchange, argumentMap);
                }
            }
        }
        catch (AMQException e)
        {
             throw new RuntimeException(e);
        }

    }

    public void completeBindingRecovery()
    {
    }

    public void complete()
    {


    }

    public void queueEntry(final UUID queueId, long messageId)
    {
        AMQQueue queue = _virtualHost.getQueueRegistry().getQueue(queueId);
        try
        {
            if(queue != null)
            {
                String queueName = queue.getName();
                ServerMessage message = _recoveredMessages.get(messageId);
                _unusedMessages.remove(messageId);

                if(message != null)
                {


                    if (_logger.isDebugEnabled())
                    {
                        _logger.debug("On recovery, delivering " + message.getMessageNumber() + " to " + queueName);
                    }

                    Integer count = _queueRecoveries.get(queueName);
                    if (count == null)
                    {
                        count = 0;
                    }

                    queue.enqueue(message);

                    _queueRecoveries.put(queueName, ++count);
                }
                else
                {
                    _logger.warn("Message id " + messageId + " referenced in log as enqueued in queue " + queueName + " is unknown, entry will be discarded");
                    Transaction txn = _store.newTransaction();
                    txn.dequeueMessage(queue, new DummyMessage(messageId));
                    txn.commitTranAsync();
                }
            }
            else
            {
                _logger.warn("Message id " + messageId + " in log references queue with id " + queueId + " which is not in the configuration, entry will be discarded");
                Transaction txn = _store.newTransaction();
                TransactionLogResource mockQueue =
                        new TransactionLogResource()
                        {
                            @Override
                            public UUID getId()
                            {
                                return queueId;
                            }
                        };
                txn.dequeueMessage(mockQueue, new DummyMessage(messageId));
                txn.commitTranAsync();
            }

        }
        catch(AMQException e)
        {
            throw new RuntimeException(e);
        }
    }

    public DtxRecordRecoveryHandler completeQueueEntryRecovery()
    {

        for(Map.Entry<String,Integer> entry : _queueRecoveries.entrySet())
        {
            CurrentActor.get().message(_logSubject, TransactionLogMessages.RECOVERED(entry.getValue(), entry.getKey()));

            CurrentActor.get().message(_logSubject, TransactionLogMessages.RECOVERY_COMPLETE(entry.getKey(), true));
        }



        return this;
    }

    private static class DummyMessage implements EnqueableMessage
    {


        private final long _messageId;

        public DummyMessage(long messageId)
        {
            _messageId = messageId;
        }

        public long getMessageNumber()
        {
            return _messageId;
        }


        public boolean isPersistent()
        {
            return true;
        }


        public StoredMessage getStoredMessage()
        {
            return null;
        }
    }

}
