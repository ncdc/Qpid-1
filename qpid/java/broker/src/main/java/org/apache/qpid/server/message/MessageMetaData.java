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
package org.apache.qpid.server.message;

import org.apache.qpid.AMQException;
import org.apache.qpid.framing.AMQShortString;
import org.apache.qpid.framing.BasicContentHeaderProperties;
import org.apache.qpid.framing.ContentHeaderBody;
import org.apache.qpid.framing.EncodingUtils;
import org.apache.qpid.framing.FieldTable;
import org.apache.qpid.framing.abstraction.MessagePublishInfo;
import org.apache.qpid.server.store.MessageMetaDataType;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.util.ByteBufferOutputStream;
import org.apache.qpid.util.ByteBufferInputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Encapsulates a publish body and a content header. In the context of the message store these are treated as a
 * single unit.
 */
public class MessageMetaData implements StorableMessageMetaData
{
    private MessagePublishInfo _messagePublishInfo;

    private ContentHeaderBody _contentHeaderBody;

    private int _contentChunkCount;

    private long _arrivalTime;
    private static final byte MANDATORY_FLAG = 1;
    private static final byte IMMEDIATE_FLAG = 2;
    public static final MessageMetaDataType.Factory<MessageMetaData> FACTORY = new MetaDataFactory();

    public MessageMetaData(MessagePublishInfo publishBody, ContentHeaderBody contentHeaderBody, int contentChunkCount)
    {
        this(publishBody,contentHeaderBody, contentChunkCount, System.currentTimeMillis());
    }

    public MessageMetaData(MessagePublishInfo publishBody, ContentHeaderBody contentHeaderBody, int contentChunkCount, long arrivalTime)
    {
        _contentHeaderBody = contentHeaderBody;
        _messagePublishInfo = publishBody;
        _contentChunkCount = contentChunkCount;
        _arrivalTime = arrivalTime;
    }

    public int getContentChunkCount()
    {
        return _contentChunkCount;
    }

    public void setContentChunkCount(int contentChunkCount)
    {
        _contentChunkCount = contentChunkCount;
    }

    public ContentHeaderBody getContentHeaderBody()
    {
        return _contentHeaderBody;
    }

    public void setContentHeaderBody(ContentHeaderBody contentHeaderBody)
    {
        _contentHeaderBody = contentHeaderBody;
    }

    public MessagePublishInfo getMessagePublishInfo()
    {
        return _messagePublishInfo;
    }

    public void setMessagePublishInfo(MessagePublishInfo messagePublishInfo)
    {
        _messagePublishInfo = messagePublishInfo;
    }

    public long getArrivalTime()
    {
        return _arrivalTime;
    }

    public void setArrivalTime(long arrivalTime)
    {
        _arrivalTime = arrivalTime;
    }

    public MessageMetaDataType getType()
    {
        return MessageMetaDataType.META_DATA_0_8;
    }

    public int getStorableSize()
    {
        int size = _contentHeaderBody.getSize();
        size += 4;
        size += EncodingUtils.encodedShortStringLength(_messagePublishInfo.getExchange());
        size += EncodingUtils.encodedShortStringLength(_messagePublishInfo.getRoutingKey());
        size += 1; // flags for immediate/mandatory
        size += EncodingUtils.encodedLongLength();

        return size;
    }


    public int writeToBuffer(int offset, ByteBuffer dest)
    {
        int oldPosition = dest.position();
        try
        {

            DataOutputStream dataOutputStream = new DataOutputStream(new ByteBufferOutputStream(dest));
            EncodingUtils.writeInteger(dataOutputStream, _contentHeaderBody.getSize());
            _contentHeaderBody.writePayload(dataOutputStream);
            EncodingUtils.writeShortStringBytes(dataOutputStream, _messagePublishInfo.getExchange());
            EncodingUtils.writeShortStringBytes(dataOutputStream, _messagePublishInfo.getRoutingKey());
            byte flags = 0;
            if(_messagePublishInfo.isMandatory())
            {
                flags |= MANDATORY_FLAG;
            }
            if(_messagePublishInfo.isImmediate())
            {
                flags |= IMMEDIATE_FLAG;
            }
            dest.put(flags);
            dest.putLong(_arrivalTime);

        }
        catch (IOException e)
        {
            // This shouldn't happen as we are not actually using anything that can throw an IO Exception
            throw new RuntimeException(e);
        }

        return dest.position()-oldPosition;
    }

    public int getContentSize()
    {
        return (int) _contentHeaderBody.getBodySize();
    }

    public boolean isPersistent()
    {
        BasicContentHeaderProperties properties = (BasicContentHeaderProperties) (_contentHeaderBody.getProperties());
        return properties.getDeliveryMode() ==  BasicContentHeaderProperties.PERSISTENT;
    }

    private static class MetaDataFactory implements MessageMetaDataType.Factory
    {


        public MessageMetaData createMetaData(ByteBuffer buf)
        {
            try
            {
                ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
                DataInputStream dais = new DataInputStream(bbis);
                int size = EncodingUtils.readInteger(dais);
                ContentHeaderBody chb = ContentHeaderBody.createFromBuffer(dais, size);
                final AMQShortString exchange = EncodingUtils.readAMQShortString(dais);
                final AMQShortString routingKey = EncodingUtils.readAMQShortString(dais);

                final byte flags = EncodingUtils.readByte(dais);
                long arrivalTime = EncodingUtils.readLong(dais);

                MessagePublishInfo publishBody =
                        new MessagePublishInfo()
                        {

                            public AMQShortString getExchange()
                            {
                                return exchange;
                            }

                            public void setExchange(AMQShortString exchange)
                            {
                            }

                            public boolean isImmediate()
                            {
                                return (flags & IMMEDIATE_FLAG) != 0;
                            }

                            public boolean isMandatory()
                            {
                                return (flags & MANDATORY_FLAG) != 0;
                            }

                            public AMQShortString getRoutingKey()
                            {
                                return routingKey;
                            }
                        };
                return new MessageMetaData(publishBody, chb, 0, arrivalTime);
            }
            catch (AMQException e)
            {
                throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

        }
    };

    public AMQMessageHeader getMessageHeader()
    {
        return new MessageHeaderAdapter();
    }

    private final class MessageHeaderAdapter implements AMQMessageHeader
    {
        private BasicContentHeaderProperties getProperties()
        {
            return (BasicContentHeaderProperties) getContentHeaderBody().getProperties();
        }

        public String getCorrelationId()
        {
            return getProperties().getCorrelationIdAsString();
        }

        public long getExpiration()
        {
            return getProperties().getExpiration();
        }

        public String getMessageId()
        {
            return getProperties().getMessageIdAsString();
        }

        public String getMimeType()
        {
            return getProperties().getContentTypeAsString();
        }

        public String getEncoding()
        {
            return getProperties().getEncodingAsString();
        }

        public byte getPriority()
        {
            return getProperties().getPriority();
        }

        public long getTimestamp()
        {
            return getProperties().getTimestamp();
        }

        public String getType()
        {
            return getProperties().getTypeAsString();
        }

        public String getReplyTo()
        {
            return getProperties().getReplyToAsString();
        }

        public String getReplyToExchange()
        {
            // TODO
            return getReplyTo();
        }

        public String getReplyToRoutingKey()
        {
            // TODO
            return getReplyTo();
        }

        public Object getHeader(String name)
        {
            FieldTable ft = getProperties().getHeaders();
            return ft.get(name);
        }

        public boolean containsHeaders(Set<String> names)
        {
            FieldTable ft = getProperties().getHeaders();
            for(String name : names)
            {
                if(!ft.containsKey(name))
                {
                    return false;
                }
            }
            return true;
        }

        public boolean containsHeader(String name)
        {
            FieldTable ft = getProperties().getHeaders();
            return ft.containsKey(name);
        }



    }
}
