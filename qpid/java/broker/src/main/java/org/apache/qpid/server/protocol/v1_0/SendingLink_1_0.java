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
package org.apache.qpid.server.protocol.v1_0;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.qpid.AMQException;
import org.apache.qpid.AMQInternalException;
import org.apache.qpid.AMQSecurityException;
import org.apache.qpid.amqp_1_0.transport.DeliveryStateHandler;
import org.apache.qpid.amqp_1_0.transport.LinkEndpoint;
import org.apache.qpid.amqp_1_0.transport.SendingLinkEndpoint;
import org.apache.qpid.amqp_1_0.transport.SendingLinkListener;
import org.apache.qpid.amqp_1_0.type.AmqpErrorException;
import org.apache.qpid.amqp_1_0.type.Binary;
import org.apache.qpid.amqp_1_0.type.DeliveryState;
import org.apache.qpid.amqp_1_0.type.Outcome;
import org.apache.qpid.amqp_1_0.type.Symbol;
import org.apache.qpid.amqp_1_0.type.UnsignedInteger;
import org.apache.qpid.amqp_1_0.type.messaging.Accepted;
import org.apache.qpid.amqp_1_0.type.messaging.ExactSubjectFilter;
import org.apache.qpid.amqp_1_0.type.messaging.Filter;
import org.apache.qpid.amqp_1_0.type.messaging.MatchingSubjectFilter;
import org.apache.qpid.amqp_1_0.type.messaging.Modified;
import org.apache.qpid.amqp_1_0.type.messaging.NoLocalFilter;
import org.apache.qpid.amqp_1_0.type.messaging.Released;
import org.apache.qpid.amqp_1_0.type.messaging.Source;
import org.apache.qpid.amqp_1_0.type.messaging.StdDistMode;
import org.apache.qpid.amqp_1_0.type.messaging.TerminusDurability;
import org.apache.qpid.amqp_1_0.type.transport.AmqpError;
import org.apache.qpid.amqp_1_0.type.transport.Detach;
import org.apache.qpid.amqp_1_0.type.transport.Error;
import org.apache.qpid.amqp_1_0.type.transport.Transfer;
import org.apache.qpid.filter.SelectorParsingException;
import org.apache.qpid.filter.selector.ParseException;
import org.apache.qpid.server.binding.Binding;
import org.apache.qpid.server.exchange.DirectExchange;
import org.apache.qpid.server.exchange.Exchange;
import org.apache.qpid.server.exchange.TopicExchange;
import org.apache.qpid.server.filter.JMSSelectorFilter;
import org.apache.qpid.server.filter.SimpleFilterManager;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.queue.AMQQueueFactory;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.txn.AutoCommitTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.virtualhost.VirtualHost;

public class SendingLink_1_0 implements SendingLinkListener, Link_1_0, DeliveryStateHandler
{
    private VirtualHost _vhost;
    private SendingDestination _destination;

    private Subscription_1_0 _subscription;
    private boolean _draining;
    private final Map<Binary, QueueEntry> _unsettledMap =
            new HashMap<Binary, QueueEntry>();

    private final ConcurrentHashMap<Binary, UnsettledAction> _unsettledActionMap =
            new ConcurrentHashMap<Binary, UnsettledAction>();
    private volatile SendingLinkAttachment _linkAttachment;
    private TerminusDurability _durability;
    private List<QueueEntry> _resumeFullTransfers = new ArrayList<QueueEntry>();
    private List<Binary> _resumeAcceptedTransfers = new ArrayList<Binary>();
    private Runnable _closeAction;

    public SendingLink_1_0(final SendingLinkAttachment linkAttachment,
                           final VirtualHost vhost,
                           final SendingDestination destination)
            throws AmqpErrorException
    {
        _vhost = vhost;
        _destination = destination;
        _linkAttachment = linkAttachment;
        final Source source = (Source) linkAttachment.getSource();
        _durability = source.getDurable();
        linkAttachment.setDeliveryStateHandler(this);
        QueueDestination qd = null;
        AMQQueue queue = null;



        boolean noLocal = false;
        JMSSelectorFilter messageFilter = null;

        if(destination instanceof QueueDestination)
        {
            queue = ((QueueDestination) _destination).getQueue();
            if(queue.getArguments() != null && queue.getArguments().containsKey("topic"))
            {
                source.setDistributionMode(StdDistMode.COPY);
            }
            qd = (QueueDestination) destination;

            Map<Symbol,Filter> filters = source.getFilter();

            Map<Symbol,Filter> actualFilters = new HashMap<Symbol,Filter>();

            if(filters != null)
            {
                for(Map.Entry<Symbol,Filter> entry : filters.entrySet())
                {
                    if(entry.getValue() instanceof NoLocalFilter)
                    {
                        actualFilters.put(entry.getKey(), entry.getValue());
                        noLocal = true;
                    }
                    else if(messageFilter == null && entry.getValue() instanceof org.apache.qpid.amqp_1_0.type.messaging.JMSSelectorFilter)
                    {

                        org.apache.qpid.amqp_1_0.type.messaging.JMSSelectorFilter selectorFilter = (org.apache.qpid.amqp_1_0.type.messaging.JMSSelectorFilter) entry.getValue();
                        try
                        {
                            messageFilter = new JMSSelectorFilter(selectorFilter.getValue());

                            actualFilters.put(entry.getKey(), entry.getValue());
                        }
                        catch (ParseException e)
                        {
                            Error error = new Error();
                            error.setCondition(AmqpError.INVALID_FIELD);
                            error.setDescription("Invalid JMS Selector: " + selectorFilter.getValue());
                            error.setInfo(Collections.singletonMap(Symbol.valueOf("field"), Symbol.valueOf("filter")));
                            throw new AmqpErrorException(error);
                        }
                        catch (SelectorParsingException e)
                        {
                            Error error = new Error();
                            error.setCondition(AmqpError.INVALID_FIELD);
                            error.setDescription("Invalid JMS Selector: " + selectorFilter.getValue());
                            error.setInfo(Collections.singletonMap(Symbol.valueOf("field"), Symbol.valueOf("filter")));
                            throw new AmqpErrorException(error);
                        }


                    }
                }
            }
            source.setFilter(actualFilters.isEmpty() ? null : actualFilters);

            _subscription = new Subscription_1_0(this, qd);
        }
        else if(destination instanceof ExchangeDestination)
        {
            try
            {

                ExchangeDestination exchangeDestination = (ExchangeDestination) destination;

                boolean isDurable = exchangeDestination.getDurability() == TerminusDurability.CONFIGURATION
                                    || exchangeDestination.getDurability() == TerminusDurability.UNSETTLED_STATE;
                String name;
                if(isDurable)
                {
                    String remoteContainerId = getEndpoint().getSession().getConnection().getRemoteContainerId();
                    remoteContainerId = remoteContainerId.replace("_","__").replace(".", "_:");

                    String endpointName = linkAttachment.getEndpoint().getName();
                    endpointName = endpointName
                                    .replace("_", "__")
                                    .replace(".", "_:")
                                    .replace("(", "_O")
                                    .replace(")", "_C")
                                    .replace("<", "_L")
                                    .replace(">", "_R");
                    name = "qpid_/" + remoteContainerId + "_/" + endpointName;
                }
                else
                {
                    name = UUID.randomUUID().toString();
                }

                queue = _vhost.getQueueRegistry().getQueue(name);
                Exchange exchange = exchangeDestination.getExchange();

                if(queue == null)
                {
                    queue = AMQQueueFactory.createAMQQueueImpl(
                                name,
                                isDurable,
                                null,
                                true,
                                true,
                                _vhost,
                                Collections.EMPTY_MAP);
                }
                else
                {
                    List<Binding> bindings = queue.getBindings();
                    List<Binding> bindingsToRemove = new ArrayList<Binding>();
                    for(Binding existingBinding : bindings)
                    {
                        if(existingBinding.getExchange() != _vhost.getExchangeRegistry().getDefaultExchange()
                            && existingBinding.getExchange() != exchange)
                        {
                            bindingsToRemove.add(existingBinding);
                        }
                    }
                    for(Binding existingBinding : bindingsToRemove)
                    {
                        existingBinding.getExchange().removeBinding(existingBinding);
                    }
                }


                String binding = "";

                Map<Symbol,Filter> filters = source.getFilter();
                Map<Symbol,Filter> actualFilters = new HashMap<Symbol,Filter>();
                boolean hasBindingFilter = false;
                if(filters != null && !filters.isEmpty())
                {

                    for(Map.Entry<Symbol,Filter> entry : filters.entrySet())
                    {
                        if(!hasBindingFilter
                           && entry.getValue() instanceof ExactSubjectFilter
                           && exchange.getType() == DirectExchange.TYPE)
                        {
                            ExactSubjectFilter filter = (ExactSubjectFilter) filters.values().iterator().next();
                            source.setFilter(filters);
                            binding = filter.getValue();
                            actualFilters.put(entry.getKey(), entry.getValue());
                            hasBindingFilter = true;
                        }
                        else if(!hasBindingFilter
                                && entry.getValue() instanceof MatchingSubjectFilter
                                && exchange.getType() == TopicExchange.TYPE)
                        {
                            MatchingSubjectFilter filter = (MatchingSubjectFilter) filters.values().iterator().next();
                            source.setFilter(filters);
                            binding = filter.getValue();
                            actualFilters.put(entry.getKey(), entry.getValue());
                            hasBindingFilter = true;
                        }
                        else if(entry.getValue() instanceof NoLocalFilter)
                        {
                            actualFilters.put(entry.getKey(), entry.getValue());
                            noLocal = true;
                        }
                        else if(messageFilter == null && entry.getValue() instanceof org.apache.qpid.amqp_1_0.type.messaging.JMSSelectorFilter)
                        {

                            org.apache.qpid.amqp_1_0.type.messaging.JMSSelectorFilter selectorFilter = (org.apache.qpid.amqp_1_0.type.messaging.JMSSelectorFilter) entry.getValue();
                            try
                            {
                                messageFilter = new JMSSelectorFilter(selectorFilter.getValue());

                                actualFilters.put(entry.getKey(), entry.getValue());
                            }
                            catch (ParseException e)
                            {
                                Error error = new Error();
                                error.setCondition(AmqpError.INVALID_FIELD);
                                error.setDescription("Invalid JMS Selector: " + selectorFilter.getValue());
                                error.setInfo(Collections.singletonMap(Symbol.valueOf("field"), Symbol.valueOf("filter")));
                                throw new AmqpErrorException(error);
                            }
                            catch (SelectorParsingException e)
                            {
                                Error error = new Error();
                                error.setCondition(AmqpError.INVALID_FIELD);
                                error.setDescription("Invalid JMS Selector: " + selectorFilter.getValue());
                                error.setInfo(Collections.singletonMap(Symbol.valueOf("field"), Symbol.valueOf("filter")));
                                throw new AmqpErrorException(error);
                            }


                        }
                    }
                }
                source.setFilter(actualFilters.isEmpty() ? null : actualFilters);

                vhost.getBindingFactory().addBinding(binding,queue,exchange,null);
                source.setDistributionMode(StdDistMode.COPY);

                qd = new QueueDestination(queue);
            }
            catch (AMQSecurityException e)
            {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            catch (AMQInternalException e)
            {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (AMQException e)
            {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            _subscription = new Subscription_1_0(this, qd, true);

        }

        if(_subscription != null)
        {
            _subscription.setNoLocal(noLocal);
            if(messageFilter!=null)
            {
                _subscription.setFilters(new SimpleFilterManager(messageFilter));
            }

            try
            {

                queue.registerSubscription(_subscription, false);
            }
            catch (AMQException e)
            {
                e.printStackTrace();  //TODO
            }
        }

    }

    public void resume(SendingLinkAttachment linkAttachment)
    {
        _linkAttachment = linkAttachment;

    }

    public void remoteDetached(final LinkEndpoint endpoint, final Detach detach)
    {
        //TODO
        // if not durable or close
        if(!TerminusDurability.UNSETTLED_STATE.equals(_durability) ||
           (detach != null && Boolean.TRUE.equals(detach.getClosed())))
        {

            AMQQueue queue = _subscription.getQueue();

            try
            {

                queue.unregisterSubscription(_subscription);

            }
            catch (AMQException e)
            {
                e.printStackTrace();  //TODO
            }

            DeliveryState state = new Modified();

            for(UnsettledAction action : _unsettledActionMap.values())
            {

                action.process(state,Boolean.TRUE);
            }
            _unsettledActionMap.clear();

            endpoint.close();

            if(_destination instanceof ExchangeDestination
               && (_durability == TerminusDurability.CONFIGURATION
                    || _durability == TerminusDurability.UNSETTLED_STATE))
            {
                try
                {
                    queue.delete();
                }
                catch(AMQException e)
                {
                    e.printStackTrace();  // TODO - Implement
                }
            }

            if(_closeAction != null)
            {
                _closeAction.run();
            }
        }
        else if(detach == null || detach.getError() != null)
        {
            _linkAttachment = null;
            _subscription.flowStateChanged();
        }
        else
        {
            endpoint.detach();
        }
    }

    public void start()
    {
        //TODO
    }

    public SendingLinkEndpoint getEndpoint()
    {
        return _linkAttachment == null ? null : _linkAttachment.getEndpoint() ;
    }

    public Session_1_0 getSession()
    {
        return _linkAttachment == null ? null : _linkAttachment.getSession();
    }

    public void flowStateChanged()
    {
        if(Boolean.TRUE.equals(getEndpoint().getDrain())
                && hasCredit())
        {
            _draining = true;
        }

        while(!_resumeAcceptedTransfers.isEmpty() && getEndpoint().hasCreditToSend())
        {
            Accepted accepted = new Accepted();
            synchronized(getLock())
            {

                Transfer xfr = new Transfer();
                Binary dt = _resumeAcceptedTransfers.remove(0);
                xfr.setDeliveryTag(dt);
                xfr.setState(accepted);
                xfr.setResume(Boolean.TRUE);
                getEndpoint().transfer(xfr);
            }

        }
        if(_resumeAcceptedTransfers.isEmpty())
        {
            _subscription.flowStateChanged();
        }

    }

    boolean hasCredit()
    {
        return getEndpoint().getLinkCredit().compareTo(UnsignedInteger.ZERO) > 0;
    }

    public boolean isDraining()
    {
        return false;  //TODO
    }

    public boolean drained()
    {
        if(getEndpoint() != null)
        {
            synchronized(getEndpoint().getLock())
            {
                if(_draining)
                {
                    //TODO
                    getEndpoint().drained();
                    _draining = false;
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }
        else
        {
            return false;
        }
    }

    public void addUnsettled(Binary tag, UnsettledAction unsettledAction, QueueEntry queueEntry)
    {
        _unsettledActionMap.put(tag,unsettledAction);
        if(getTransactionId() == null)
        {
            _unsettledMap.put(tag, queueEntry);
        }
    }

    public void removeUnsettled(Binary tag)
    {
        _unsettledActionMap.remove(tag);
    }

    public void handle(Binary deliveryTag, DeliveryState state, Boolean settled)
    {
        UnsettledAction action = _unsettledActionMap.get(deliveryTag);
        boolean localSettle = false;
        if(action != null)
        {
            localSettle = action.process(state, settled);
            if(localSettle && !Boolean.TRUE.equals(settled))
            {
                _linkAttachment.updateDisposition(deliveryTag, state, true);
            }
        }
        if(Boolean.TRUE.equals(settled) || localSettle)
        {
            _unsettledActionMap.remove(deliveryTag);
            _unsettledMap.remove(deliveryTag);
        }
    }

    ServerTransaction getTransaction(Binary transactionId)
    {
        return _linkAttachment.getSession().getTransaction(transactionId);
    }

    public Binary getTransactionId()
    {
        SendingLinkEndpoint endpoint = getEndpoint();
        return endpoint == null ? null : endpoint.getTransactionId();
    }

    public synchronized Object getLock()
    {
        return _linkAttachment == null ? this : getEndpoint().getLock();
    }

    public boolean isDetached()
    {
        return _linkAttachment == null || getEndpoint().isDetached();
    }

    public boolean isAttached()
    {
        return _linkAttachment != null && getEndpoint().isAttached();
    }

    public synchronized void setLinkAttachment(SendingLinkAttachment linkAttachment)
    {

        if(_subscription.isActive())
        {
            _subscription.suspend();
        }

        _linkAttachment = linkAttachment;

        SendingLinkEndpoint endpoint = linkAttachment.getEndpoint();
        endpoint.setDeliveryStateHandler(this);
        Map initialUnsettledMap = endpoint.getInitialUnsettledMap();
        Map<Binary, QueueEntry> unsettledCopy = new HashMap<Binary, QueueEntry>(_unsettledMap);
        _resumeAcceptedTransfers.clear();
        _resumeFullTransfers.clear();

        for(Map.Entry<Binary, QueueEntry> entry : unsettledCopy.entrySet())
        {
            Binary deliveryTag = entry.getKey();
            final QueueEntry queueEntry = entry.getValue();
            if(initialUnsettledMap == null || !initialUnsettledMap.containsKey(deliveryTag))
            {
                queueEntry.setRedelivered();
                queueEntry.release();
                _unsettledMap.remove(deliveryTag);
            }
            else if(initialUnsettledMap != null && (initialUnsettledMap.get(deliveryTag) instanceof Outcome))
            {
                Outcome outcome = (Outcome) initialUnsettledMap.get(deliveryTag);

                if(outcome instanceof Accepted)
                {
                    AutoCommitTransaction txn = new AutoCommitTransaction(_vhost.getMessageStore());
                    if(_subscription.acquires())
                    {
                        txn.dequeue(Collections.singleton(queueEntry),
                                new ServerTransaction.Action()
                                {
                                    public void postCommit()
                                    {
                                        queueEntry.discard();
                                    }

                                    public void onRollback()
                                    {
                                        //To change body of implemented methods use File | Settings | File Templates.
                                    }
                                });
                    }
                }
                else if(outcome instanceof Released)
                {
                    AutoCommitTransaction txn = new AutoCommitTransaction(_vhost.getMessageStore());
                    if(_subscription.acquires())
                    {
                        txn.dequeue(Collections.singleton(queueEntry),
                                new ServerTransaction.Action()
                                {
                                    public void postCommit()
                                    {
                                        queueEntry.release();
                                    }

                                    public void onRollback()
                                    {
                                        //To change body of implemented methods use File | Settings | File Templates.
                                    }
                                });
                    }
                }
                //_unsettledMap.remove(deliveryTag);
                initialUnsettledMap.remove(deliveryTag);
                _resumeAcceptedTransfers.add(deliveryTag);
            }
            else
            {
                _resumeFullTransfers.add(queueEntry);
                // exists in receivers map, but not yet got an outcome ... should resend with resume = true
            }
            // TODO - else
        }


    }

    public Map getUnsettledOutcomeMap()
    {
        Map<Binary, QueueEntry> unsettled = new HashMap<Binary, QueueEntry>(_unsettledMap);

        for(Map.Entry<Binary, QueueEntry> entry : unsettled.entrySet())
        {
            entry.setValue(null);
        }

        return unsettled;
    }

    public void setCloseAction(Runnable action)
    {
        _closeAction = action;
    }
}
