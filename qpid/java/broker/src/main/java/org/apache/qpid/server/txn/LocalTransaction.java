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
package org.apache.qpid.server.txn;

import org.apache.qpid.server.store.StoreFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.AMQException;
import org.apache.qpid.server.message.EnqueableMessage;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A concrete implementation of ServerTransaction where enqueue/dequeue
 * operations share a single long-lived transaction.
 * 
 * The caller is responsible for invoking commit() (or rollback()) as necessary.
 */
public class LocalTransaction implements ServerTransaction
{
    protected static final Logger _logger = LoggerFactory.getLogger(LocalTransaction.class);

    private final List<Action> _postTransactionActions = new ArrayList<Action>();

    private volatile Transaction _transaction;
    private MessageStore _transactionLog;
    private long _txnStartTime = 0L;
    private StoreFuture _asyncTran;

    public LocalTransaction(MessageStore transactionLog)
    {
        _transactionLog = transactionLog;
    }
    
    public boolean inTransaction()
    {
        return _transaction != null;
    }

    public long getTransactionStartTime()
    {
        return _txnStartTime;
    }

    public void addPostTransactionAction(Action postTransactionAction)
    {
        sync();
        _postTransactionActions.add(postTransactionAction);
    }

    public void dequeue(BaseQueue queue, EnqueableMessage message, Action postTransactionAction)
    {
        sync();
        _postTransactionActions.add(postTransactionAction);

        if(message.isPersistent() && queue.isDurable())
        {
            try
            {
                if (_logger.isDebugEnabled())
                {
                    _logger.debug("Dequeue of message number " + message.getMessageNumber() + " from transaction log. Queue : " + queue.getNameShortString());
                }

                beginTranIfNecessary();
                _transaction.dequeueMessage(queue, message);

            }
            catch(AMQException e)
            {
                _logger.error("Error during message dequeues", e);
                tidyUpOnError(e);
            }
        }
    }

    public void dequeue(Collection<QueueEntry> queueEntries, Action postTransactionAction)
    {
        sync();
        _postTransactionActions.add(postTransactionAction);

        try
        {
            for(QueueEntry entry : queueEntries)
            {
                ServerMessage message = entry.getMessage();
                BaseQueue queue = entry.getQueue();

                if(message.isPersistent() && queue.isDurable())
                {
                    if (_logger.isDebugEnabled())
                    {
                        _logger.debug("Dequeue of message number " + message.getMessageNumber() + " from transaction log. Queue : " + queue.getNameShortString());
                    }

                    beginTranIfNecessary();
                    _transaction.dequeueMessage(queue, message);
                }

            }
        }
        catch(AMQException e)
        {
            _logger.error("Error during message dequeues", e);
            tidyUpOnError(e);
        }
    }

    private void tidyUpOnError(Exception e)
    {
        try
        {
            doRollbackActions();
        }
        finally
        {
            try
            {
                if (_transaction != null)
                {
                    _transaction.abortTran();
                }
            }
            catch (Exception abortException)
            {
                _logger.error("Abort transaction failed while trying to handle previous error", abortException);
            }
            finally
            {
		        resetDetails();
            }
        }

        throw new RuntimeException(e);
    }

    private void beginTranIfNecessary()
    {

        if(_transaction == null)
        {
            try
            {
                _transaction = _transactionLog.newTransaction();
            }
            catch (Exception e)
            {
                tidyUpOnError(e);
            }
        }
    }

    public void enqueue(BaseQueue queue, EnqueableMessage message, Action postTransactionAction)
    {
        sync();
        _postTransactionActions.add(postTransactionAction);

        if(message.isPersistent() && queue.isDurable())
        {
            try
            {
                if (_logger.isDebugEnabled())
                {
                    _logger.debug("Enqueue of message number " + message.getMessageNumber() + " to transaction log. Queue : " + queue.getNameShortString());
                }
                
                beginTranIfNecessary();
                _transaction.enqueueMessage(queue, message);
            }
            catch (Exception e)
            {
                _logger.error("Error during message enqueue", e);

                tidyUpOnError(e);
            }
        }
    }

    public void enqueue(List<? extends BaseQueue> queues, EnqueableMessage message, Action postTransactionAction, long currentTime)
    {
        sync();
        _postTransactionActions.add(postTransactionAction);

        if (_txnStartTime == 0L)
        {
            _txnStartTime = currentTime == 0L ? System.currentTimeMillis() : currentTime;
        }

        if(message.isPersistent())
        {
            try
            {
                for(BaseQueue queue : queues)
                {
                    if(queue.isDurable())
                    {
                        if (_logger.isDebugEnabled())
                        {
                            _logger.debug("Enqueue of message number " + message.getMessageNumber() + " to transaction log. Queue : " + queue.getNameShortString() );
                        }
                        
                        
                        beginTranIfNecessary();
                        _transaction.enqueueMessage(queue, message);
                    }
                }

            }
            catch (Exception e)
            {
                _logger.error("Error during message enqueue", e);

                tidyUpOnError(e);
            }
        }
    }

    public void commit()
    {
        sync();
        commit(null);
    }

    public void commit(Runnable immediateAction)
    {
        sync();
        try
        {
            if(_transaction != null)
            {
                _transaction.commitTran();
            }

            if(immediateAction != null)
            {
                immediateAction.run();
            }

            doPostTransactionActions();
        }
        catch (Exception e)
        {
            _logger.error("Failed to commit transaction", e);

            doRollbackActions();
            throw new RuntimeException("Failed to commit transaction", e);
        }
        finally
        {
            resetDetails();
        }
    }

    private void doRollbackActions()
    {
        for(Action action : _postTransactionActions)
        {
            action.onRollback();
        }
    }

    public StoreFuture commitAsync(final Runnable deferred)
    {
        sync();
        try
        {
            StoreFuture future = StoreFuture.IMMEDIATE_FUTURE;
            if(_transaction != null)
            {
                future = new StoreFuture()
                        {
                            private volatile boolean _completed = false;
                            private StoreFuture _underlying = _transaction.commitTranAsync();

                            @Override
                            public boolean isComplete()
                            {
                                return _completed || checkUnderlyingCompletion();
                            }

                            @Override
                            public void waitForCompletion()
                            {
                                if(!_completed)
                                {
                                    _underlying.waitForCompletion();
                                    checkUnderlyingCompletion();
                                }
                            }

                            private synchronized boolean checkUnderlyingCompletion()
                            {
                                if(!_completed && _underlying.isComplete())
                                {
                                    completeDeferredWork();
                                    _completed = true;
                                }
                                return _completed;

                            }

                            private void completeDeferredWork()
                            {
                                try
                                {
                                    doPostTransactionActions();
                                    deferred.run();

                                }
                                catch (Exception e)
                                {
                                    _logger.error("Failed to commit transaction", e);

                                    doRollbackActions();
                                    throw new RuntimeException("Failed to commit transaction", e);
                                }
                                finally
                                {
                                    resetDetails();
                                }
                            }

                };
                _asyncTran = future;
            }
            else
            {
                try
                {
                    doPostTransactionActions();

                    deferred.run();
                }
                finally
                {
                    resetDetails();
                }
            }

            return future;
        }
        catch (Exception e)
        {
            _logger.error("Failed to commit transaction", e);
            try
            {
                doRollbackActions();
            }
            finally
            {
                resetDetails();
            }
            throw new RuntimeException("Failed to commit transaction", e);
        }


    }

    private void doPostTransactionActions()
    {
        for(int i = 0; i < _postTransactionActions.size(); i++)
        {
            _postTransactionActions.get(i).postCommit();
        }
    }

    public void rollback()
    {
        sync();
        try
        {
            if(_transaction != null)
            {
                _transaction.abortTran();
            }
        }
        catch (AMQException e)
        {
            _logger.error("Failed to rollback transaction", e);
            throw new RuntimeException("Failed to rollback transaction", e);
        }
        finally
        {
            try
            {
                doRollbackActions();
            }
            finally
            {
                resetDetails();
            }
        }
    }

    public void sync()
    {
        if(_asyncTran != null)
        {
            _asyncTran.waitForCompletion();
            _asyncTran = null;
        }
    }

    private void resetDetails()
    {
        _asyncTran = null;
        _transaction = null;
	    _postTransactionActions.clear();
        _txnStartTime = 0L;
    }

    public boolean isTransactional()
    {
        return true;
    }
}
