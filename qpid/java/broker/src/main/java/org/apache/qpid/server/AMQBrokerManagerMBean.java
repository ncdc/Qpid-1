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
package org.apache.qpid.server;

import org.apache.qpid.AMQException;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.management.common.mbeans.ManagedBroker;
import org.apache.qpid.management.common.mbeans.ManagedQueue;
import org.apache.qpid.management.common.mbeans.annotations.MBeanConstructor;
import org.apache.qpid.management.common.mbeans.annotations.MBeanDescription;
import org.apache.qpid.server.exchange.Exchange;
import org.apache.qpid.server.exchange.ExchangeFactory;
import org.apache.qpid.server.exchange.ExchangeRegistry;
import org.apache.qpid.server.exchange.ExchangeType;
import org.apache.qpid.server.logging.actors.CurrentActor;
import org.apache.qpid.server.logging.actors.ManagementActor;
import org.apache.qpid.server.management.AMQManagedObject;
import org.apache.qpid.server.management.ManagedObject;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.queue.AMQQueueFactory;
import org.apache.qpid.server.queue.AMQQueueMBean;
import org.apache.qpid.server.queue.QueueRegistry;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.virtualhost.VirtualHost;
import org.apache.qpid.server.virtualhost.VirtualHostImpl;

import javax.management.JMException;
import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This MBean implements the broker management interface and exposes the
 * Broker level management features like creating and deleting exchanges and queue.
 */
@MBeanDescription("This MBean exposes the broker level management features")
public class AMQBrokerManagerMBean extends AMQManagedObject implements ManagedBroker
{
    private final QueueRegistry _queueRegistry;
    private final ExchangeRegistry _exchangeRegistry;
    private final ExchangeFactory _exchangeFactory;
    private final Exchange _defaultExchange;
    private final DurableConfigurationStore _durableConfig;

    private final VirtualHostImpl.VirtualHostMBean _virtualHostMBean;

    @MBeanConstructor("Creates the Broker Manager MBean")
    public AMQBrokerManagerMBean(VirtualHostImpl.VirtualHostMBean virtualHostMBean) throws JMException
    {
        super(ManagedBroker.class, ManagedBroker.TYPE);

        _virtualHostMBean = virtualHostMBean;
        VirtualHost virtualHost = virtualHostMBean.getVirtualHost();

        _queueRegistry = virtualHost.getQueueRegistry();
        _exchangeRegistry = virtualHost.getExchangeRegistry();
        _defaultExchange = _exchangeRegistry.getDefaultExchange();
        _durableConfig = virtualHost.getMessageStore();
        _exchangeFactory = virtualHost.getExchangeFactory();
    }

    public String getObjectInstanceName()
    {
        return _virtualHostMBean.getVirtualHost().getName();
    }

    /**
     * Returns an array of the exchange types available for creation.
     * @since Qpid JMX API 1.3
     * @throws IOException
     */
    public String[] getExchangeTypes() throws IOException
    {
        ArrayList<String> exchangeTypes = new ArrayList<String>();
        for(ExchangeType<? extends Exchange> ex : _exchangeFactory.getPublicCreatableTypes())
        {
            exchangeTypes.add(ex.getName().toString());
        }

        return exchangeTypes.toArray(new String[0]);
    }

    /**
     * Returns a list containing the names of the attributes available for the Queue mbeans.
     * @since Qpid JMX API 1.3
     * @throws IOException
     */
    public List<String> retrieveQueueAttributeNames() throws IOException
    {
        return ManagedQueue.QUEUE_ATTRIBUTES;
    }

    /**
     * Returns a List of Object Lists containing the requested attribute values (in the same sequence requested) for each queue in the virtualhost.
     * If a particular attribute cant be found or raises an mbean/reflection exception whilst being gathered its value is substituted with the String "-".
     * @since Qpid JMX API 1.3
     * @throws IOException
     */
    public List<List<Object>> retrieveQueueAttributeValues(String[] attributes) throws IOException
    {
        if(_queueRegistry.getQueues().size() == 0)
        {
            return new ArrayList<List<Object>>();
        }

        List<List<Object>> queueAttributesList = new ArrayList<List<Object>>(_queueRegistry.getQueues().size());

        int attributesLength = attributes.length;

        for(AMQQueue queue : _queueRegistry.getQueues())
        {
            AMQQueueMBean mbean = (AMQQueueMBean) queue.getManagedObject();

            if(mbean == null)
            {
                continue;
            }

            List<Object> attributeValues = new ArrayList<Object>(attributesLength);

            for(int i=0; i < attributesLength; i++)
            {
                try
                {
                    attributeValues.add(mbean.getAttribute(attributes[i]));
                }
                catch (Exception e)
                {
                    attributeValues.add("-");
                }
            }

            queueAttributesList.add(attributeValues);
        }

        return queueAttributesList;
    }

    /**
     * Creates new exchange and registers it with the registry.
     *
     * @param exchangeName
     * @param type
     * @param durable
     * @throws JMException
     * @throws MBeanException
     */
    public void createNewExchange(String exchangeName, String type, boolean durable) throws JMException, MBeanException
    {
        CurrentActor.set(new ManagementActor(getLogActor().getRootMessageLogger()));
        try
        {
            synchronized (_exchangeRegistry)
            {
                Exchange exchange = _exchangeRegistry.getExchange(new AMQShortString(exchangeName));
                if (exchange == null)
                {
                    exchange = _exchangeFactory.createExchange(new AMQShortString(exchangeName), new AMQShortString(type),
                                                               durable, false, 0);
                    _exchangeRegistry.registerExchange(exchange);
                    if (durable)
                    {
                        _durableConfig.createExchange(exchange);
                    }
                }
                else
                {
                    throw new JMException("The exchange \"" + exchangeName + "\" already exists.");
                }
            }
        }
        catch (AMQException ex)
        {
            JMException jme = new JMException(ex.toString());
            throw new MBeanException(jme, "Error in creating exchange " + exchangeName);
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    /**
     * Unregisters the exchange from registry.
     *
     * @param exchangeName
     * @throws JMException
     * @throws MBeanException
     */
    public void unregisterExchange(String exchangeName) throws JMException, MBeanException
    {
        // TODO
        // Check if the exchange is in use.

        // Check if there are queue-bindings with the exchange and unregister
        // when there are no bindings.
        CurrentActor.set(new ManagementActor(getLogActor().getRootMessageLogger()));
        try
        {
            _exchangeRegistry.unregisterExchange(new AMQShortString(exchangeName), false);
        }
        catch (AMQException ex)
        {
            JMException jme = new JMException(ex.toString());
            throw new MBeanException(jme, "Error in unregistering exchange " + exchangeName);
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    /**
     * Creates a new queue and registers it with the registry and puts it
     * in persistance storage if durable queue.
     *
     * @param queueName
     * @param durable
     * @param owner
     * @throws JMException
     * @throws MBeanException
     */
    public void createNewQueue(String queueName, String owner, boolean durable) throws JMException, MBeanException
    {
        createNewQueue(queueName, owner, durable, null);
    }

    public void createNewQueue(String queueName, String owner, boolean durable, Map<String,Object> arguments) throws JMException
    {
        final AMQShortString queueNameAsAMQShortString = new AMQShortString(queueName);
        AMQQueue queue = _queueRegistry.getQueue(queueNameAsAMQShortString);
        if (queue != null)
        {
            throw new JMException("The queue \"" + queueName + "\" already exists.");
        }

        CurrentActor.set(new ManagementActor(getLogActor().getRootMessageLogger()));
        try
        {
            AMQShortString ownerShortString = null;
            if (owner != null)
            {
                ownerShortString = new AMQShortString(owner);
            }

            FieldTable args = null;
            if(arguments != null)
            {
                args = FieldTable.convertToFieldTable(arguments);
            }
            final VirtualHost virtualHost = getVirtualHost();

            queue = AMQQueueFactory.createAMQQueueImpl(queueNameAsAMQShortString, durable, ownerShortString,
                                                       false, false, getVirtualHost(), args);
            if (queue.isDurable() && !queue.isAutoDelete())
            {
                _durableConfig.createQueue(queue, args);
            }

            virtualHost.getBindingFactory().addBinding(queueName, queue, _defaultExchange, null);
        }
        catch (AMQException ex)
        {
            JMException jme = new JMException(ex.toString());
            throw new MBeanException(jme, "Error in creating queue " + queueName);
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    private VirtualHost getVirtualHost()
    {
        return _virtualHostMBean.getVirtualHost();
    }

    /**
     * Deletes the queue from queue registry and persistant storage.
     *
     * @param queueName
     * @throws JMException
     * @throws MBeanException
     */
    public void deleteQueue(String queueName) throws JMException, MBeanException
    {
        AMQQueue queue = _queueRegistry.getQueue(new AMQShortString(queueName));
        if (queue == null)
        {
            throw new JMException("The Queue " + queueName + " is not a registered queue.");
        }

        CurrentActor.set(new ManagementActor(getLogActor().getRootMessageLogger()));
        try
        {
            queue.delete();
            if (queue.isDurable())
            {
                _durableConfig.removeQueue(queue);
            }
        }
        catch (AMQException ex)
        {
            JMException jme = new JMException(ex.toString());
            throw new MBeanException(jme, "Error in deleting queue " + queueName);
        }
        finally
        {
            CurrentActor.remove();
        }
    }

    @Override
    public ManagedObject getParentObject()
    {
        return _virtualHostMBean;
    }

    // This will have a single instance for a virtual host, so not having the name property in the ObjectName
    @Override
    public ObjectName getObjectName() throws MalformedObjectNameException
    {
        return getObjectNameForSingleInstanceMBean();
    }

    public void resetStatistics() throws Exception
    {
        getVirtualHost().resetStatistics();
    }

    public double getPeakMessageDeliveryRate()
    {
        return getVirtualHost().getMessageDeliveryStatistics().getPeak();
    }

    public double getPeakDataDeliveryRate()
    {
        return getVirtualHost().getDataDeliveryStatistics().getPeak();
    }

    public double getMessageDeliveryRate()
    {
        return getVirtualHost().getMessageDeliveryStatistics().getRate();
    }

    public double getDataDeliveryRate()
    {
        return getVirtualHost().getDataDeliveryStatistics().getRate();
    }

    public long getTotalMessagesDelivered()
    {
        return getVirtualHost().getMessageDeliveryStatistics().getTotal();
    }

    public long getTotalDataDelivered()
    {
        return getVirtualHost().getDataDeliveryStatistics().getTotal();
    }

    public double getPeakMessageReceiptRate()
    {
        return getVirtualHost().getMessageReceiptStatistics().getPeak();
    }

    public double getPeakDataReceiptRate()
    {
        return getVirtualHost().getDataReceiptStatistics().getPeak();
    }

    public double getMessageReceiptRate()
    {
        return getVirtualHost().getMessageReceiptStatistics().getRate();
    }

    public double getDataReceiptRate()
    {
        return getVirtualHost().getDataReceiptStatistics().getRate();
    }

    public long getTotalMessagesReceived()
    {
        return getVirtualHost().getMessageReceiptStatistics().getTotal();
    }

    public long getTotalDataReceived()
    {
        return getVirtualHost().getDataReceiptStatistics().getTotal();
    }

    public boolean isStatisticsEnabled()
    {
        return getVirtualHost().isStatisticsEnabled();
    }
}
