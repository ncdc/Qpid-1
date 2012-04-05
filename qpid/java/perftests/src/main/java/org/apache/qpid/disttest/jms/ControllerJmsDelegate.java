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
 *
 */
package org.apache.qpid.disttest.jms;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.NamingException;

import org.apache.qpid.disttest.DistributedTestException;
import org.apache.qpid.disttest.controller.CommandListener;
import org.apache.qpid.disttest.controller.config.QueueConfig;
import org.apache.qpid.disttest.message.Command;
import org.apache.qpid.disttest.message.RegisterClientCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerJmsDelegate
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerJmsDelegate.class);

    private final Map<String, Destination> _clientNameToQueueMap = new ConcurrentHashMap<String, Destination>();
    private final Connection _connection;
    private final Destination _controllerQueue;
    private final Session _session;
    private final QueueCreator _queueCreator;

    private List<CommandListener> _commandListeners = new CopyOnWriteArrayList<CommandListener>();

    public ControllerJmsDelegate(final Context context) throws NamingException, JMSException
    {
        final ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("connectionfactory");
        _connection = connectionFactory.createConnection();
        _connection.start();
        _controllerQueue = (Destination) context.lookup("controllerqueue");
        _session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        _queueCreator = new QpidQueueCreator();
    }

    public void start()
    {
        try
        {
            final MessageConsumer consumer = _session.createConsumer(_controllerQueue);
            consumer.setMessageListener(new MessageListener()
            {
                @Override
                public void onMessage(final Message message)
                {
                    try
                    {
                        String jmsMessageID = message.getJMSMessageID();
                        LOGGER.debug("Received message " + jmsMessageID);

                        final Command command = JmsMessageAdaptor.messageToCommand(message);
                        LOGGER.debug("Converted message " + jmsMessageID + " into command: " + command);

                        processCommandWithFirstSupportingListener(command);
                        LOGGER.debug("Finished processing command for message " + jmsMessageID);
                    }
                    catch (Throwable t)
                    {
                        LOGGER.error("Can't handle JMS message", t);
                    }
                }
            });
        }
        catch (final JMSException e)
        {
            throw new DistributedTestException(e);
        }
    }

    /** ensures connections are closed, otherwise the JVM may be prevented from terminating */
    public void closeConnections()
    {
        try
        {
            _session.close();
        }
        catch (JMSException e)
        {
            LOGGER.error("Unable to close session", e);
        }

        try
        {
            _connection.stop();
        }
        catch (JMSException e)
        {
            LOGGER.error("Unable to stop connection", e);
        }

        try
        {
            _connection.close();
        }
        catch (JMSException e)
        {
            throw new DistributedTestException("Unable to close connection", e);
        }
    }

    public void registerClient(final RegisterClientCommand command)
    {
        final String clientName = command.getClientName();
        final Destination clientIntructionQueue = createDestinationFromString(command.getClientQueueName());
        _clientNameToQueueMap.put(clientName, clientIntructionQueue);
    }

    public void sendCommandToClient(final String clientName, final Command command)
    {
        final Destination clientQueue = _clientNameToQueueMap.get(clientName);
        if (clientQueue == null)
        {
            throw new DistributedTestException("Client name " + clientName + " not known. I know about: "
                            + _clientNameToQueueMap.keySet());
        }

        try
        {
            final MessageProducer producer = _session.createProducer(clientQueue);
            final Message message = JmsMessageAdaptor.commandToMessage(_session, command);

            producer.send(message);
        }
        catch (final JMSException e)
        {
            throw new DistributedTestException(e);
        }
    }

    private void processCommandWithFirstSupportingListener(Command command)
    {
        for (CommandListener listener : _commandListeners)
        {
            if (listener.supports(command))
            {
                listener.processCommand(command);
                return;
            }
        }

        throw new IllegalStateException("There is no registered listener to process command " + command);
    }

    private Destination createDestinationFromString(final String clientQueueName)
    {
        Destination clientIntructionQueue;
        try
        {
            clientIntructionQueue = _session.createQueue(clientQueueName);
        }
        catch (JMSException e)
        {
            throw new DistributedTestException("Unable to create Destination from " + clientQueueName);
        }
        return clientIntructionQueue;
    }

    public void createQueues(List<QueueConfig> queues)
    {
        _queueCreator.createQueues(_connection, queues);
    }

    public void deleteQueues(List<QueueConfig> queues)
    {
        _queueCreator.deleteQueues(_connection, queues);
    }

    public void addCommandListener(CommandListener commandListener)
    {
        _commandListeners.add(commandListener);
    }

    public void removeCommandListener(CommandListener commandListener)
    {
        _commandListeners.remove(commandListener);
    }
}
