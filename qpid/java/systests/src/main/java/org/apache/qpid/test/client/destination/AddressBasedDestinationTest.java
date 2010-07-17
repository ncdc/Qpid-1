package org.apache.qpid.test.client.destination;
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


import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.Context;

import org.apache.qpid.client.AMQAnyDestination;
import org.apache.qpid.client.AMQDestination;
import org.apache.qpid.client.AMQSession_0_10;
import org.apache.qpid.client.messaging.address.Node.ExchangeNode;
import org.apache.qpid.client.messaging.address.Node.QueueNode;
import org.apache.qpid.jndi.PropertiesFileInitialContextFactory;
import org.apache.qpid.messaging.Address;
import org.apache.qpid.test.utils.QpidBrokerTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddressBasedDestinationTest extends QpidBrokerTestCase
{
    private static final Logger _logger = LoggerFactory.getLogger(AddressBasedDestinationTest.class);
    private Connection _connection;
    
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        _connection = getConnection() ;
        _connection.start();
    }
    
    @Override
    public void tearDown() throws Exception
    {
        _connection.close();
        super.tearDown();
    }
    
    public void testCreateOptions() throws Exception
    {
        Session jmsSession = _connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        MessageProducer prod;
        MessageConsumer cons;
        
        // default (create never, assert never) -------------------
        // create never --------------------------------------------
        String addr1 = "ADDR:testQueue1";
        AMQDestination  dest = new AMQAnyDestination(addr1);
        try
        {
            cons = jmsSession.createConsumer(dest);
        }
        catch(JMSException e)
        {
            assertTrue(e.getMessage().contains("The name 'testQueue1' supplied in the address " +
                    "doesn't resolve to an exchange or a queue"));
        }
        
        try
        {
            prod = jmsSession.createProducer(dest);
        }
        catch(JMSException e)
        {
            assertTrue(e.getMessage().contains("The name supplied in the address " +
                    "doesn't resolve to an exchange or a queue"));
        }
            
        assertFalse("Queue should not be created",(
                (AMQSession_0_10)jmsSession).isQueueExist(dest, (QueueNode)dest.getSourceNode() ,true));
        
        
        // create always -------------------------------------------
        addr1 = "ADDR:testQueue1; { create: always }";
        dest = new AMQAnyDestination(addr1);
        cons = jmsSession.createConsumer(dest); 
        
        assertTrue("Queue not created as expected",(
                (AMQSession_0_10)jmsSession).isQueueExist(dest,(QueueNode)dest.getSourceNode(), true));              
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("", 
                    dest.getAddressName(),dest.getAddressName(), dest.getSourceNode().getDeclareArgs()));
        
        // create receiver -----------------------------------------
        addr1 = "ADDR:testQueue2; { create: receiver }";
        dest = new AMQAnyDestination(addr1);
        try
        {
            prod = jmsSession.createProducer(dest);
        }
        catch(JMSException e)
        {
            assertTrue(e.getMessage().contains("The name supplied in the address " +
                    "doesn't resolve to an exchange or a queue"));
        }
            
        assertFalse("Queue should not be created",(
                (AMQSession_0_10)jmsSession).isQueueExist(dest,(QueueNode)dest.getSourceNode(), true));
        
        
        cons = jmsSession.createConsumer(dest); 
        
        assertTrue("Queue not created as expected",(
                (AMQSession_0_10)jmsSession).isQueueExist(dest,(QueueNode)dest.getSourceNode(), true));              
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("", 
                    dest.getAddressName(),dest.getAddressName(), dest.getSourceNode().getDeclareArgs()));
        
        // create never --------------------------------------------
        addr1 = "ADDR:testQueue3; { create: never }";
        dest = new AMQAnyDestination(addr1);
        try
        {
            cons = jmsSession.createConsumer(dest); 
        }
        catch(JMSException e)
        {
            assertTrue(e.getMessage().contains("The name 'testQueue3' supplied in the address " +
                    "doesn't resolve to an exchange or a queue"));
        }
        
        try
        {
            prod = jmsSession.createProducer(dest);
        }
        catch(JMSException e)
        {
            assertTrue(e.getMessage().contains("The name 'testQueue3' supplied in the address " +
                    "doesn't resolve to an exchange or a queue"));
        }
            
        assertFalse("Queue should not be created",(
                (AMQSession_0_10)jmsSession).isQueueExist(dest,(QueueNode)dest.getSourceNode(), true));
        
        // create sender ------------------------------------------
        addr1 = "ADDR:testQueue3; { create: sender }";
        dest = new AMQAnyDestination(addr1);
                
        try
        {
            cons = jmsSession.createConsumer(dest); 
        }
        catch(JMSException e)
        {
            assertTrue(e.getMessage().contains("The name 'testQueue3' supplied in the address " +
                    "doesn't resolve to an exchange or a queue"));
        }
        assertFalse("Queue should not be created",(
                (AMQSession_0_10)jmsSession).isQueueExist(dest,(QueueNode)dest.getSourceNode(), true));
        
        prod = jmsSession.createProducer(dest);
        assertTrue("Queue not created as expected",(
                (AMQSession_0_10)jmsSession).isQueueExist(dest,(QueueNode)dest.getSourceNode(), true));              
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("", 
                    dest.getAddressName(),dest.getAddressName(), dest.getSourceNode().getDeclareArgs()));
        
    }
    
    // todo add tests for delete options
    
    public void testCreateQueue() throws Exception
    {
        Session jmsSession = _connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        
        String addr = "ADDR:my-queue/hello; " +
                      "{" + 
                            "create: always, " +
                            "node: " + 
                            "{" + 
                                 "durable: true ," +
                                 "x-declare: " +
                                 "{" + 
                                     "auto-delete: true," +
                                     "'qpid.max_size': 1000," +
                                     "'qpid.max_count': 100" +
                                  "}, " +   
                                  "x-bindings: [{exchange : 'amq.direct', key : test}, " + 
                                               "{exchange : 'amq.fanout'}," +
                                               "{exchange: 'amq.match', arguments: {x-match: any, dep: sales, loc: CA}}," +
                                               "{exchange : 'amq.topic', key : 'a.#'}" +
                                              "]," + 
                                     
                            "}" +
                      "}";
        AMQDestination dest = new AMQAnyDestination(addr);
        MessageConsumer cons = jmsSession.createConsumer(dest); 
        
        assertTrue("Queue not created as expected",(
                (AMQSession_0_10)jmsSession).isQueueExist(dest,(QueueNode)dest.getSourceNode(), true));              
        
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("", 
                    dest.getAddressName(),dest.getAddressName(), null));
        
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("amq.direct", 
                    dest.getAddressName(),"test", null));
        
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("amq.fanout", 
                    dest.getAddressName(),null, null));
        
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("amq.topic", 
                    dest.getAddressName(),"a.#", null));   
        
        Map<String,Object> args = new HashMap<String,Object>();
        args.put("x-match","any");
        args.put("dep","sales");
        args.put("loc","CA");
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("amq.match", 
                    dest.getAddressName(),null, args));
        
    }
    
    public void testCreateExchange() throws Exception
    {
        Session jmsSession = _connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        
        String addr = "ADDR:my-exchange/hello; " + 
                      "{ " + 
                        "create: always, " +                        
                        "node: " + 
                        "{" +
                             "type: topic, " +
                             "x-declare: " +
                             "{ " + 
                                 "type:direct, " + 
                                 "auto-delete: true, " +
                                 "'qpid.msg_sequence': 1, " +
                                 "'qpid.ive': 1" + 
                             "}" +
                        "}" +
                      "}";
        
        AMQDestination dest = new AMQAnyDestination(addr);
        MessageConsumer cons = jmsSession.createConsumer(dest); 
        
        assertTrue("Exchange not created as expected",(
                (AMQSession_0_10)jmsSession).isExchangeExist(dest, (ExchangeNode)dest.getTargetNode() , true));
       
        // The existence of the queue is implicitly tested here
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("my-exchange", 
                    dest.getQueueName(),"hello", Collections.<String, Object>emptyMap()));
    }
    
    public void testBindQueueWithArgs() throws Exception
    {
        Session jmsSession = _connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        
        String headersBinding = "{exchange: 'amq.match', arguments: {x-match: any, dep: sales, loc: CA}}";
        
        String addr = "ADDR:my-queue/hello; " + 
                      "{ " + 
                           "create: always, " +
                           "node: "  + 
                           "{" + 
                               "durable: true ," +
                               "x-declare: " + 
                               "{ " + 
                                     "auto-delete: true," +
                                     "'qpid.max_count': 100" +
                               "}, " +
                               "x-bindings: [{exchange : 'amq.direct', key : test}, " +
                                            "{exchange : 'amq.topic', key : 'a.#'}," + 
                                             headersBinding + 
                                           "]" +
                           "}" +
                      "}";

        AMQDestination dest = new AMQAnyDestination(addr);
        MessageConsumer cons = jmsSession.createConsumer(dest); 
        
        assertTrue("Queue not created as expected",(
                (AMQSession_0_10)jmsSession).isQueueExist(dest,(QueueNode)dest.getSourceNode(), true));              
        
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("", 
                    dest.getAddressName(),dest.getAddressName(), null));
        
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("amq.direct", 
                    dest.getAddressName(),"test", null));  
      
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("amq.topic", 
                    dest.getAddressName(),"a.#", null));
        
        Address a = Address.parse(headersBinding);
        assertTrue("Queue not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("amq.match", 
                    dest.getAddressName(),null, a.getOptions()));
    }
    
    /**
     * Test goal: Verifies the capacity property in address string is handled properly.
     * Test strategy:
     * Creates a destination with capacity 10.
     * Creates consumer with client ack.
     * Sends 15 messages to the queue, tries to receive 10.
     * Tries to receive the 11th message and checks if its null.
     * 
     * Since capacity is 10 and we haven't acked any messages, 
     * we should not have received the 11th.
     * 
     * Acks the 10th message and verifies we receive the rest of the msgs.
     */
    public void testCapacity() throws Exception
    {
        verifyCapacity("ADDR:my-queue; {create: always, link:{capacity: 10}}");
    }
    
    public void testSourceAndTargetCapacity() throws Exception
    {
        verifyCapacity("ADDR:my-queue; {create: always, link:{capacity: {source:10, target:15} }}");
    }
    
    private void verifyCapacity(String address) throws Exception
    {
        if (!isCppBroker())
        {
            _logger.info("Not C++ broker, exiting test");
            return;
        }
        
        Session jmsSession = _connection.createSession(false,Session.CLIENT_ACKNOWLEDGE);
        
        AMQDestination dest = new AMQAnyDestination(address);
        MessageConsumer cons = jmsSession.createConsumer(dest); 
        MessageProducer prod = jmsSession.createProducer(dest);
        
        for (int i=0; i< 15; i++)
        {
            prod.send(jmsSession.createTextMessage("msg" + i) );
        }
        
        for (int i=0; i< 9; i++)
        {
            cons.receive();
        }
        Message msg = cons.receive(RECEIVE_TIMEOUT);
        assertNotNull("Should have received the 10th message",msg);        
        assertNull("Shouldn't have received the 11th message as capacity is 10",cons.receive(RECEIVE_TIMEOUT));
        msg.acknowledge();
        for (int i=11; i<16; i++)
        {
            assertNotNull("Should have received the " + i + "th message as we acked the last 10",cons.receive(RECEIVE_TIMEOUT));
        }
    }
    
    /**
     * Test goal: Verifies if the new address format based destinations
     *            can be specified and loaded correctly from the properties file.
     * 
     */
    public void testLoadingFromPropertiesFile() throws Exception
    {
        Hashtable<String,String> map = new Hashtable<String,String>();        
        map.put("destination.myQueue1", "ADDR:my-queue/hello; {create: always, node: " + 
                "{x-declare: {auto-delete: true,'qpid.max_size': 1000}}}");
        
        map.put("destination.myQueue2", "ADDR:my-queue2; { create: receiver }");

        map.put("destination.myQueue3", "BURL:direct://amq.direct/my-queue3?routingkey='test'");
        
        PropertiesFileInitialContextFactory props = new PropertiesFileInitialContextFactory();
        Context ctx = props.getInitialContext(map);
        
        AMQDestination dest1 = (AMQDestination)ctx.lookup("myQueue1");      
        AMQDestination dest2 = (AMQDestination)ctx.lookup("myQueue2");
        AMQDestination dest3 = (AMQDestination)ctx.lookup("myQueue3");
        
        Session jmsSession = _connection.createSession(false,Session.CLIENT_ACKNOWLEDGE);
        MessageConsumer cons1 = jmsSession.createConsumer(dest1); 
        MessageConsumer cons2 = jmsSession.createConsumer(dest2);
        MessageConsumer cons3 = jmsSession.createConsumer(dest3);
        
        assertTrue("Destination1 was not created as expected",(
                (AMQSession_0_10)jmsSession).isQueueExist(dest1,(QueueNode)dest1.getSourceNode(), true));              
        
        assertTrue("Destination1 was not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("", 
                    dest1.getAddressName(),dest1.getAddressName(), null));
        
        assertTrue("Destination2 was not created as expected",(
                (AMQSession_0_10)jmsSession).isQueueExist(dest2,(QueueNode)dest2.getSourceNode(), true));              
        
        assertTrue("Destination2 was not bound as expected",(
                (AMQSession_0_10)jmsSession).isQueueBound("", 
                    dest2.getAddressName(),dest2.getAddressName(), null));
        
        MessageProducer producer = jmsSession.createProducer(dest3);
        producer.send(jmsSession.createTextMessage("Hello"));
        TextMessage msg = (TextMessage)cons3.receive(1000);
        assertEquals("Destination3 was not created as expected.",msg.getText(),"Hello");
    }

    /**
     * Test goal: Verifies the subject can be overridden using "qpid.subject" message property.
     * Test strategy: Creates and address with a default subject "topic1"
     *                Creates a message with "qpid.subject"="topic2" and sends it.
     *                Verifies that the message goes to "topic2" instead of "topic1". 
     */
    public void testOverridingSubject() throws Exception
    {
        Session jmsSession = _connection.createSession(false,Session.CLIENT_ACKNOWLEDGE);
        
        AMQDestination topic1 = new AMQAnyDestination("ADDR:amq.topic/topic1; {link:{name: queue1}}");
        MessageProducer prod = jmsSession.createProducer(topic1);
        
        Message m = jmsSession.createTextMessage("Hello");
        m.setStringProperty("qpid.subject", "topic2");
        
        MessageConsumer consForTopic1 = jmsSession.createConsumer(topic1);
        MessageConsumer consForTopic2 = jmsSession.createConsumer(new AMQAnyDestination("ADDR:amq.topic/topic2; {link:{name: queue2}}"));
        
        prod.send(m);
        Message msg = consForTopic1.receive(1000);
        assertNull("message shouldn't have been sent to topic1",msg);
        
        msg = consForTopic2.receive(1000);
        assertNotNull("message should have been sent to topic2",msg);        
        
    }
    
    /**
    * Test goal: Verifies that and address based destination can be used successfully 
    *            as a reply to.
    */
    public void testAddressBasedReplyTo() throws Exception
    {
        Session jmsSession = _connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        
        String addr = "ADDR:amq.direct/x512; {create: receiver, " +
                      "link : {name : 'MY.RESP.QUEUE', " + 
                      "x-declare : { auto-delete: true, exclusive: true, " +
                                   "'qpid.max_size': 1000, 'qpid.policy_type': ring } } }";
        
        Destination replyTo = new AMQAnyDestination(addr);
        Destination dest =new AMQAnyDestination("ADDR:amq.direct/Hello");
        
        MessageConsumer cons = jmsSession.createConsumer(dest);                
        MessageProducer prod = jmsSession.createProducer(dest);
        Message m = jmsSession.createTextMessage("Hello");
        m.setJMSReplyTo(replyTo);
        prod.send(m);
        
        Message msg = cons.receive(1000);
        assertNotNull("consumer should have received the message",msg);
        
        MessageConsumer replyToCons = jmsSession.createConsumer(replyTo);
        MessageProducer replyToProd = jmsSession.createProducer(msg.getJMSReplyTo());
        replyToProd.send(jmsSession.createTextMessage("reply"));
        
        Message replyToMsg = replyToCons.receive(1000);
        assertNotNull("The reply to consumer should have got the message",replyToMsg);        
    }
    
    /**
     * Test goal: Verifies that session.createQueue method
     *            works as expected both with the new and old addressing scheme.
     */
    public void testSessionCreateQueue() throws Exception
    {
        Session ssn = _connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        
        // Using the BURL method
        Destination queue = ssn.createQueue("my-queue");
        MessageProducer prod = ssn.createProducer(queue); 
        MessageConsumer cons = ssn.createConsumer(queue);
        assertTrue("my-queue was not created as expected",(
                (AMQSession_0_10)ssn).isQueueBound("amq.direct", 
                    "my-queue","my-queue", null));
        
        prod.send(ssn.createTextMessage("test"));
        assertNotNull("consumer should receive a message",cons.receive(1000));
        cons.close();
        
        // Using the ADDR method
        queue = ssn.createQueue("ADDR:my-queue2");
        prod = ssn.createProducer(queue); 
        cons = ssn.createConsumer(queue);
        assertTrue("my-queue2 was not created as expected",(
                (AMQSession_0_10)ssn).isQueueBound("", 
                    "my-queue2","my-queue2", null));
        
        prod.send(ssn.createTextMessage("test"));
        assertNotNull("consumer should receive a message",cons.receive(1000));
        cons.close();
        
        // Using the ADDR method to create a more complicated queue
        String addr = "ADDR:amq.direct/x512; {create: receiver, " +
        "link : {name : 'MY.RESP.QUEUE', " + 
        "x-declare : { auto-delete: true, exclusive: true, " +
                     "'qpid.max_size': 1000, 'qpid.policy_type': ring } } }";
        queue = ssn.createQueue(addr);
        
        prod = ssn.createProducer(queue); 
        cons = ssn.createConsumer(queue);
        assertTrue("MY.RESP.QUEUE was not created as expected",(
                (AMQSession_0_10)ssn).isQueueBound("amq.direct", 
                    "MY.RESP.QUEUE","x512", null));
        cons.close();
    }
    
    /**
     * Test goal: Verifies that session.creatTopic method
     *            works as expected both with the new and old addressing scheme.
     */
    public void testSessionCreateTopic() throws Exception
    {
        Session ssn = _connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        
        // Using the BURL method
        Topic topic = ssn.createTopic("ACME");
        MessageProducer prod = ssn.createProducer(topic); 
        MessageConsumer cons = ssn.createConsumer(topic);
        
        prod.send(ssn.createTextMessage("test"));
        assertNotNull("consumer should receive a message",cons.receive(1000));
        cons.close();
     
        // Using the ADDR method
        topic = ssn.createTopic("ADDR:ACME");
        prod = ssn.createProducer(topic); 
        cons = ssn.createConsumer(topic);
        
        prod.send(ssn.createTextMessage("test"));
        assertNotNull("consumer should receive a message",cons.receive(1000));
        cons.close();
        
        String addr = "ADDR:vehicles/bus; " + 
        "{ " + 
          "create: always, " +                        
          "node: " + 
          "{" +
               "type: topic, " +
               "x-declare: " +
               "{ " + 
                   "type:direct, " + 
                   "auto-delete: true, " +
                   "'qpid.msg_sequence': 1, " +
                   "'qpid.ive': 1" + 
               "}" +
          "}, " +
          "link: {name : my-topic, " +
              "x-bindings: [{exchange : 'vehicles', key : car}, " +
                           "{exchange : 'vehicles', key : van}]" + 
          "}" + 
        "}";
        
        // Using the ADDR method to create a more complicated topic
        topic = ssn.createTopic(addr);
        prod = ssn.createProducer(topic); 
        cons = ssn.createConsumer(topic);
        
        assertTrue("The queue was not bound to vehicle exchange using bus as the binding key",(
                (AMQSession_0_10)ssn).isQueueBound("vehicles", 
                    "my-topic","bus", null));
        
        assertTrue("The queue was not bound to vehicle exchange using car as the binding key",(
                (AMQSession_0_10)ssn).isQueueBound("vehicles", 
                    "my-topic","car", null));
        
        assertTrue("The queue was not bound to vehicle exchange using van as the binding key",(
                (AMQSession_0_10)ssn).isQueueBound("vehicles", 
                    "my-topic","van", null));
        
        Message msg = ssn.createTextMessage("test");
        msg.setStringProperty("qpid.subject", "van");
        prod.send(msg);
        assertNotNull("consumer should receive a message",cons.receive(1000));
        cons.close();
    }
    
    /**
     * The default for amq.topic is "#" and for the rest it's ""
     */
    public void testDefaultSubjects() throws Exception
    {
        Session ssn = _connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        
        MessageConsumer queueCons = ssn.createConsumer(new AMQAnyDestination("ADDR:amq.direct"));
        MessageConsumer topicCons = ssn.createConsumer(new AMQAnyDestination("ADDR:amq.topic"));
        
        MessageProducer queueProducer = ssn.createProducer(new AMQAnyDestination("ADDR:amq.direct"));
        MessageProducer topicProducer1 = ssn.createProducer(new AMQAnyDestination("ADDR:amq.topic/usa.weather"));
        MessageProducer topicProducer2 = ssn.createProducer(new AMQAnyDestination("ADDR:amq.topic/sales"));
        
        queueProducer.send(ssn.createBytesMessage());
        assertNotNull("The consumer subscribed to amq.direct " +
        		"with empty binding key should have received the message ",queueCons.receive(1000));
        
        topicProducer1.send(ssn.createTextMessage("25c"));
        assertEquals("The consumer subscribed to amq.topic " +
                "with '#' binding key should have received the message ",
                ((TextMessage)topicCons.receive(1000)).getText(),"25c");
        
        topicProducer2.send(ssn.createTextMessage("1000"));
        assertEquals("The consumer subscribed to amq.topic " +
                "with '#' binding key should have received the message ",
                ((TextMessage)topicCons.receive(1000)).getText(),"1000");
    }
}
