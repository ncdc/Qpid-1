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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.BasicContentHeaderProperties;
import org.apache.qpid.framing.ContentHeaderBody;
import org.apache.qpid.framing.MethodRegistry;
import org.apache.qpid.framing.ProtocolVersion;
import org.apache.qpid.framing.abstraction.MessagePublishInfo;
import org.apache.qpid.framing.abstraction.MessagePublishInfoImpl;
import org.apache.qpid.server.message.EnqueableMessage;
import org.apache.qpid.server.message.MessageMetaData;
import org.apache.qpid.test.utils.QpidTestCase;
import org.apache.qpid.util.FileUtils;

public abstract class MessageStoreQuotaEventsTestBase extends QpidTestCase implements EventListener, TransactionLogResource
{
    private static final Logger _logger = Logger.getLogger(MessageStoreQuotaEventsTestBase.class);

    protected static final byte[] MESSAGE_DATA = new byte[32 * 1024];

    private MessageStore _store;
    private File _storeLocation;

    private List<Event> _events;
    private UUID _transactionResource;

    protected abstract MessageStore createStore() throws Exception;

    protected abstract void applyStoreSpecificConfiguration(XMLConfiguration config);

    protected abstract int getNumberOfMessagesToFillStore();

    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        _storeLocation = new File(new File(TMP_FOLDER), getTestName());
        FileUtils.delete(_storeLocation, true);

        XMLConfiguration config = new XMLConfiguration();
        config.addProperty("environment-path", _storeLocation.getAbsolutePath());
        applyStoreSpecificConfiguration(config);

        _store = createStore();
        _store.configureConfigStore("test", null, config);

        _transactionResource = UUID.randomUUID();
        _events = new ArrayList<Event>();
        _store.addEventListener(this, Event.PERSISTENT_MESSAGE_SIZE_OVERFULL, Event.PERSISTENT_MESSAGE_SIZE_UNDERFULL);
    }

    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();
        FileUtils.delete(_storeLocation, true);
    }

    public void testOverflow() throws Exception
    {
        Transaction transaction = _store.newTransaction();

        List<EnqueableMessage> messages = new ArrayList<EnqueableMessage>();
        for (int i = 0; i < getNumberOfMessagesToFillStore(); i++)
        {
            EnqueableMessage m = addMessage(i);
            messages.add(m);
            transaction.enqueueMessage(this, m);
        }
        transaction.commitTran();

        assertEvent(1, Event.PERSISTENT_MESSAGE_SIZE_OVERFULL);

        for (EnqueableMessage m : messages)
        {
            m.getStoredMessage().remove();
        }

        assertEvent(2, Event.PERSISTENT_MESSAGE_SIZE_UNDERFULL);
    }

    protected EnqueableMessage addMessage(long id)
    {
        MessagePublishInfo pubInfoBody = new MessagePublishInfoImpl(new AMQShortString(getName()), false, false,
                new AMQShortString(getName()));
        BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        props.setDeliveryMode(Integer.valueOf(BasicContentHeaderProperties.PERSISTENT).byteValue());
        props.setContentType(getTestName());

        MethodRegistry methodRegistry = MethodRegistry.getMethodRegistry(ProtocolVersion.v0_9);
        int classForBasic = methodRegistry.createBasicQosOkBody().getClazz();
        ContentHeaderBody contentHeaderBody = new ContentHeaderBody(classForBasic, 1, props, MESSAGE_DATA.length);

        MessageMetaData metaData = new MessageMetaData(pubInfoBody, contentHeaderBody, 1);
        StoredMessage<MessageMetaData> handle = _store.addMessage(metaData);
        handle.addContent(0, ByteBuffer.wrap(MESSAGE_DATA));
        TestMessage message = new TestMessage(id, handle);
        return message;
    }

    @Override
    public void event(Event event)
    {
        _logger.debug("Test event listener received event " + event);
        _events.add(event);
    }

    private void assertEvent(int expectedNumberOfEvents, Event... expectedEvents)
    {
        assertEquals("Unexpected number of events received ", expectedNumberOfEvents, _events.size());
        for (Event event : expectedEvents)
        {
            assertTrue("Expected event is not found:" + event, _events.contains(event));
        }
    }

    @Override
    public UUID getId()
    {
        return _transactionResource;
    }

    private static class TestMessage implements EnqueableMessage
    {
        private final StoredMessage<?> _handle;
        private final long _messageId;

        public TestMessage(long messageId, StoredMessage<?> handle)
        {
            _messageId = messageId;
            _handle = handle;
        }

        public long getMessageNumber()
        {
            return _messageId;
        }

        public boolean isPersistent()
        {
            return true;
        }

        public StoredMessage<?> getStoredMessage()
        {
            return _handle;
        }
    }
}
