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
package org.apache.qpid.jca.example.ejb;

import java.util.Date;

import javax.annotation.Resource;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MessageDriven(activationConfig = {
    @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge"),
    @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
    @ActivationConfigProperty(propertyName = "destination", propertyValue = "@qpid.request.queue.jndi.name@"),
    @ActivationConfigProperty(propertyName = "connectionURL", propertyValue = "@broker.url@"),
    @ActivationConfigProperty(propertyName = "maxSession", propertyValue = "10")
})
public class QpidJMSResponderBean implements MessageListener
{

    private static final Logger _log = LoggerFactory.getLogger(QpidJMSResponderBean.class);

    @Resource(@jndi.scheme@="@qpid.xacf.jndi.name@")
    private ConnectionFactory _connectionFactory;

    @Override
    public void onMessage(Message message)
    {
        Connection connection = null;
        Session session = null;
        MessageProducer messageProducer = null;
        TextMessage response = null;

        try
        {
            if(message instanceof TextMessage)
            {
                String content = ((TextMessage)message).getText();

                _log.info("Received text message with contents: [" + content + "] at " + new Date());

                StringBuffer temp = new StringBuffer();
                temp.append("QpidJMSResponderBean received message with content: [" + content);
                temp.append("] at " + new Date());

                connection = _connectionFactory.createConnection();
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                if(message.getJMSReplyTo() != null)
                {
                    _log.info("Sending response via JMSReplyTo");
                    messageProducer = session.createProducer(message.getJMSReplyTo());
                    response = session.createTextMessage();
                    response.setText(temp.toString());
                    messageProducer.send(response);
                }
                else
                {
                    _log.info("JMSReplyTo is null. Will not respond to message.");
                }


            }
        }
        catch(Exception e)
        {
            _log.error(e.getMessage(), e);
        }
        finally
        {
            QpidUtil.closeResources(session, connection);
        }
    }
}
