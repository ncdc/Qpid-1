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
package org.apache.qpid.server.transport;

import com.sun.security.auth.UserPrincipal;

import org.apache.qpid.AMQException;
import org.apache.qpid.server.configuration.ConfigStore;
import org.apache.qpid.server.configuration.ConfiguredObject;
import org.apache.qpid.server.configuration.ConnectionConfig;
import org.apache.qpid.server.configuration.SessionConfig;
import org.apache.qpid.server.configuration.SessionConfigType;
import org.apache.qpid.server.message.ServerMessage;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.queue.BaseQueue;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.security.PrincipalHolder;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.subscription.Subscription_0_10;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.virtualhost.VirtualHost;
import org.apache.qpid.transport.Binary;
import org.apache.qpid.transport.Connection;
import org.apache.qpid.transport.MessageTransfer;
import org.apache.qpid.transport.Method;
import org.apache.qpid.transport.Range;
import org.apache.qpid.transport.RangeSet;
import org.apache.qpid.transport.Session;
import org.apache.qpid.transport.SessionDelegate;
import static org.apache.qpid.util.Serial.gt;

import java.lang.ref.WeakReference;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ServerSession extends Session implements PrincipalHolder, SessionConfig
{
    private static final String NULL_DESTINTATION = UUID.randomUUID().toString();

    private final UUID _id;
    private ConnectionConfig _connectionConfig;

    public static interface MessageDispositionChangeListener
    {
        public void onAccept();

        public void onRelease();

        public void onReject();

        public boolean acquire();


    }

    public static interface Task
    {
        public void doTask(ServerSession session);
    }


    private final SortedMap<Integer, MessageDispositionChangeListener> _messageDispositionListenerMap =
            new ConcurrentSkipListMap<Integer, MessageDispositionChangeListener>();

    private ServerTransaction _transaction;

    private Principal _principal;

    private Map<String, Subscription_0_10> _subscriptions = new ConcurrentHashMap<String, Subscription_0_10>();

    private final List<Task> _taskList = new CopyOnWriteArrayList<Task>();

    private final WeakReference<Session> _reference;

    ServerSession(Connection connection, SessionDelegate delegate, Binary name, long expiry)
    {
        this(connection, delegate, name, expiry, ((ServerConnection)connection).getConfig());
    }

    public ServerSession(Connection connection, SessionDelegate delegate, Binary name, long expiry, ConnectionConfig connConfig)
    {
        super(connection, delegate, name, expiry);
        _connectionConfig = connConfig;        
        _transaction = new AutoCommitTransaction(this.getMessageStore());
        _principal = new UserPrincipal(connection.getAuthorizationID());
        _reference = new WeakReference(this);
        _id = getConfigStore().createId();
        getConfigStore().addConfiguredObject(this);
    }

    private ConfigStore getConfigStore()
    {
        return getConnectionConfig().getConfigStore();
    }


    @Override
    protected boolean isFull(int id)
    {
        return isCommandsFull(id);
    }

    public void enqueue(final ServerMessage message, final ArrayList<? extends BaseQueue> queues)
    {

            _transaction.enqueue(queues,message, new ServerTransaction.Action()
            {

                BaseQueue[] _queues = queues.toArray(new BaseQueue[queues.size()]);

                public void postCommit()
                {
                    for(int i = 0; i < _queues.length; i++)
                    {
                        try
                        {
                            _queues[i].enqueue(message);
                        }
                        catch (AMQException e)
                        {
                            // TODO
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                            throw new RuntimeException(e);
                        }
                    }
                }

                public void onRollback()
                {
                    // NO-OP
                }
            });


    }


    public void sendMessage(MessageTransfer xfr,
                            Runnable postIdSettingAction)
    {
        invoke(xfr, postIdSettingAction);
    }

    public void onMessageDispositionChange(MessageTransfer xfr, MessageDispositionChangeListener acceptListener)
    {
        _messageDispositionListenerMap.put(xfr.getId(), acceptListener);
    }


    private static interface MessageDispositionAction
    {
        void performAction(MessageDispositionChangeListener  listener);
    }

    public void accept(RangeSet ranges)
    {
        dispositionChange(ranges, new MessageDispositionAction()
                                      {
                                          public void performAction(MessageDispositionChangeListener listener)
                                          {
                                              listener.onAccept();
                                          }
                                      });
    }


    public void release(RangeSet ranges)
    {
        dispositionChange(ranges, new MessageDispositionAction()
                                      {
                                          public void performAction(MessageDispositionChangeListener listener)
                                          {
                                              listener.onRelease();
                                          }
                                      });
    }

    public void reject(RangeSet ranges)
    {
        dispositionChange(ranges, new MessageDispositionAction()
                                      {
                                          public void performAction(MessageDispositionChangeListener listener)
                                          {
                                              listener.onReject();
                                          }
                                      });
    }

    public RangeSet acquire(RangeSet transfers)
    {
        RangeSet acquired = new RangeSet();

        if(!_messageDispositionListenerMap.isEmpty())
        {
            Iterator<Integer> unacceptedMessages = _messageDispositionListenerMap.keySet().iterator();
            Iterator<Range> rangeIter = transfers.iterator();

            if(rangeIter.hasNext())
            {
                Range range = rangeIter.next();

                while(range != null && unacceptedMessages.hasNext())
                {
                    int next = unacceptedMessages.next();
                    while(gt(next, range.getUpper()))
                    {
                        if(rangeIter.hasNext())
                        {
                            range = rangeIter.next();
                        }
                        else
                        {
                            range = null;
                            break;
                        }
                    }
                    if(range != null && range.includes(next))
                    {
                        MessageDispositionChangeListener changeListener = _messageDispositionListenerMap.get(next);
                        if(changeListener.acquire())
                        {
                            acquired.add(next);
                        }
                    }


                }

            }


        }

        return acquired;
    }

    public void dispositionChange(RangeSet ranges, MessageDispositionAction action)
    {
        if(ranges != null && !_messageDispositionListenerMap.isEmpty())
        {
            Iterator<Integer> unacceptedMessages = _messageDispositionListenerMap.keySet().iterator();
            Iterator<Range> rangeIter = ranges.iterator();

            if(rangeIter.hasNext())
            {
                Range range = rangeIter.next();

                while(range != null && unacceptedMessages.hasNext())
                {
                    int next = unacceptedMessages.next();
                    while(gt(next, range.getUpper()))
                    {
                        if(rangeIter.hasNext())
                        {
                            range = rangeIter.next();
                        }
                        else
                        {
                            range = null;
                            break;
                        }
                    }
                    if(range != null && range.includes(next))
                    {
                        MessageDispositionChangeListener changeListener = _messageDispositionListenerMap.remove(next);
                        action.performAction(changeListener);
                    }


                }

            }

        }
    }

    public void removeDispositionListener(Method method)
    {
        _messageDispositionListenerMap.remove(method.getId());
    }

    public void onClose()
    {
        _transaction.rollback();
        for(MessageDispositionChangeListener listener : _messageDispositionListenerMap.values())
        {
            listener.onRelease();
        }
        _messageDispositionListenerMap.clear();

        getConfigStore().removeConfiguredObject(this);

        for (Task task : _taskList)
        {
            task.doTask(this);
        }

    }

    public void acknowledge(final Subscription_0_10 sub, final QueueEntry entry)
    {
        _transaction.dequeue(entry.getQueue(), entry.getMessage(),
                             new ServerTransaction.Action()
                             {

                                 public void postCommit()
                                 {
                                     sub.acknowledge(entry);
                                 }

                                 public void onRollback()
                                 {
                                     entry.release();
                                 }
                             });
    }

    public Collection<Subscription_0_10> getSubscriptions()
    {
        return _subscriptions.values();
    }

    public void register(String destination, Subscription_0_10 sub)
    {
        _subscriptions.put(destination == null ? NULL_DESTINTATION : destination, sub);
    }

    public Subscription_0_10 getSubscription(String destination)
    {
        return _subscriptions.get(destination == null ? NULL_DESTINTATION : destination);
    }

    public void unregister(Subscription_0_10 sub)
    {
        _subscriptions.remove(sub.getConsumerTag().toString());
        try
        {
            sub.getSendLock();
            AMQQueue queue = sub.getQueue();
            if(queue != null)
            {
                queue.unregisterSubscription(sub);
            }

        }
        catch (AMQException e)
        {
            // TODO
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        finally
        {
            sub.releaseSendLock();
        }
    }

    public void selectTx()
    {
        _transaction = new LocalTransaction(this.getMessageStore());
    }

    public void commit()
    {
        _transaction.commit();
    }

    public void rollback()
    {
        _transaction.rollback();
    }

    public Principal getPrincipal()
    {
        return _principal;
    }

    public void addSessionCloseTask(Task task)
    {
        _taskList.add(task);
    }

    public void removeSessionCloseTask(Task task)
    {
        _taskList.remove(task);
    }

    public WeakReference<Session> getReference()
     {
         return _reference;
     }

    public MessageStore getMessageStore()
    {
        return getVirtualHost().getMessageStore();
    }

    public VirtualHost getVirtualHost()
    {
        return (VirtualHost) _connectionConfig.getVirtualHost();
    }

    public UUID getId()
    {
        return _id;
    }

    public SessionConfigType getConfigType()
    {
        return SessionConfigType.getInstance();
    }

    public ConfiguredObject getParent()
    {
        return getVirtualHost();
    }

    public boolean isDurable()
    {
        return false;
    }

    public boolean isAttached()
    {
        return true;
    }

    public long getDetachedLifespan()
    {
        return 0;
    }

    public Long getExpiryTime()
    {
        return null;
    }

    public Long getMaxClientRate()
    {
        return null;
    }

    public ConnectionConfig getConnectionConfig()
    {
        return _connectionConfig;
    }

    public String getSessionName()
    {
        return getName().toString();
    }
}
