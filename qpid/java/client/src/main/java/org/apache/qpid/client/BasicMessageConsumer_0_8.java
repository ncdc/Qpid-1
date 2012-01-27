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
package org.apache.qpid.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.AMQException;
import org.apache.qpid.client.failover.FailoverException;
import org.apache.qpid.client.message.AMQMessageDelegateFactory;
import org.apache.qpid.client.message.AbstractJMSMessage;
import org.apache.qpid.client.message.MessageFactoryRegistry;
import org.apache.qpid.client.message.UnprocessedMessage_0_8;
import org.apache.qpid.client.protocol.AMQProtocolHandler;
import org.apache.qpid.common.AMQPFilterTypes;
import org.apache.qpid.configuration.ClientProperties;
import org.apache.qpid.framing.AMQFrame;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.BasicCancelBody;
import org.apache.qpid.framing.BasicCancelOkBody;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.jms.ConnectionURL;

import javax.jms.JMSException;
import javax.jms.Message;

public class BasicMessageConsumer_0_8 extends BasicMessageConsumer<UnprocessedMessage_0_8>
{
    private final Logger _logger = LoggerFactory.getLogger(getClass());
    private AMQSession_0_8.DestinationCache<AMQTopic> _topicDestinationCache;
    private AMQSession_0_8.DestinationCache<AMQQueue> _queueDestinationCache;

    private final RejectBehaviour _rejectBehaviour;

    protected BasicMessageConsumer_0_8(int channelId, AMQConnection connection, AMQDestination destination,
                                       String messageSelector, boolean noLocal, MessageFactoryRegistry messageFactory, AMQSession_0_8 session,
                                       AMQProtocolHandler protocolHandler, FieldTable rawSelector, int prefetchHigh, int prefetchLow,
                                       boolean exclusive, int acknowledgeMode, boolean browseOnly, boolean autoClose) throws JMSException
    {
        super(channelId, connection, destination,messageSelector,noLocal,messageFactory,session,
              protocolHandler, rawSelector, prefetchHigh, prefetchLow, exclusive,
              acknowledgeMode, browseOnly, autoClose);
        final FieldTable consumerArguments = getArguments();
        if (isAutoClose())
        {
            consumerArguments.put(AMQPFilterTypes.AUTO_CLOSE.getValue(), Boolean.TRUE);
        }

        if (isBrowseOnly())
        {
            consumerArguments.put(AMQPFilterTypes.NO_CONSUME.getValue(), Boolean.TRUE);
        }

        _topicDestinationCache = session.getTopicDestinationCache();
        _queueDestinationCache = session.getQueueDestinationCache();

        if (destination.getRejectBehaviour() != null)
        {
            _rejectBehaviour = destination.getRejectBehaviour();
        }
        else
        {
            ConnectionURL connectionURL = connection.getConnectionURL();
            String rejectBehaviour = connectionURL.getOption(ConnectionURL.OPTIONS_REJECT_BEHAVIOUR);
            if (rejectBehaviour != null)
            {
                _rejectBehaviour = RejectBehaviour.valueOf(rejectBehaviour.toUpperCase());
            }
            else
            {
                // use the default value for all connections, if not set
                rejectBehaviour = System.getProperty(ClientProperties.REJECT_BEHAVIOUR_PROP_NAME, RejectBehaviour.NORMAL.toString());
                _rejectBehaviour = RejectBehaviour.valueOf( rejectBehaviour.toUpperCase());
            }
        }
    }

    void sendCancel() throws AMQException, FailoverException
    {
        BasicCancelBody body = getSession().getMethodRegistry().createBasicCancelBody(new AMQShortString(String.valueOf(getConsumerTag())), false);

        final AMQFrame cancelFrame = body.generateFrame(getChannelId());

        getProtocolHandler().syncWrite(cancelFrame, BasicCancelOkBody.class);

        if (_logger.isDebugEnabled())
        {
            _logger.debug("CancelOk'd for consumer:" + debugIdentity());
        }
    }

    public AbstractJMSMessage createJMSMessageFromUnprocessedMessage(AMQMessageDelegateFactory delegateFactory, UnprocessedMessage_0_8 messageFrame)throws Exception
    {

        return getMessageFactory().createMessage(messageFrame.getDeliveryTag(),
                messageFrame.isRedelivered(), messageFrame.getExchange(),
                messageFrame.getRoutingKey(), messageFrame.getContentHeader(), messageFrame.getBodies(),
                _queueDestinationCache, _topicDestinationCache);

    }

    Message receiveBrowse() throws JMSException
    {
        return receive();
    }

    void cleanupQueue() throws AMQException, FailoverException
    {
        
    }

    public RejectBehaviour getRejectBehaviour()
    {
        return _rejectBehaviour;
    }
}
