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
package org.apache.qpid.framing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.codec.MarkableDataInput;

import java.io.IOException;

public class AMQDataBlockDecoder
{

    private static final BodyFactory[] _bodiesSupported = new BodyFactory[Byte.MAX_VALUE];

    static
    {
        _bodiesSupported[ContentHeaderBody.TYPE] = ContentHeaderBodyFactory.getInstance();
        _bodiesSupported[ContentBody.TYPE] = ContentBodyFactory.getInstance();
        _bodiesSupported[HeartbeatBody.TYPE] = new HeartbeatBodyFactory();
    }

    private Logger _logger = LoggerFactory.getLogger(AMQDataBlockDecoder.class);

    public AMQDataBlockDecoder()
    { }

    public boolean decodable(MarkableDataInput in) throws AMQFrameDecodingException, IOException
    {
        final int remainingAfterAttributes = in.available() - (1 + 2 + 4 + 1);
        // type, channel, body length and end byte
        if (remainingAfterAttributes < 0)
        {
            return false;
        }

        in.mark(8);
        in.skip(1 + 2);


        // Get an unsigned int, lifted from MINA ByteBuffer getUnsignedInt() 
        final long bodySize = in.readInt() & 0xffffffffL;

        in.reset();

        return (remainingAfterAttributes >= bodySize);

    }

    public AMQFrame createAndPopulateFrame(AMQMethodBodyFactory methodBodyFactory, MarkableDataInput in)
            throws AMQFrameDecodingException, AMQProtocolVersionException, IOException
    {
        final byte type = in.readByte();

        BodyFactory bodyFactory;
        if (type == AMQMethodBody.TYPE)
        {
            bodyFactory = methodBodyFactory;
        }
        else
        {
            bodyFactory = _bodiesSupported[type];
        }

        if (bodyFactory == null)
        {
            throw new AMQFrameDecodingException(null, "Unsupported frame type: " + type, null);
        }

        final int channel = in.readUnsignedShort();
        final long bodySize = EncodingUtils.readUnsignedInteger(in);

        // bodySize can be zero
        if ((channel < 0) || (bodySize < 0))
        {
            throw new AMQFrameDecodingException(null, "Undecodable frame: type = " + type + " channel = " + channel
                + " bodySize = " + bodySize, null);
        }

        AMQFrame frame = new AMQFrame(in, channel, bodySize, bodyFactory);

        byte marker = in.readByte();
        if ((marker & 0xFF) != 0xCE)
        {
            throw new AMQFrameDecodingException(null, "End of frame marker not found. Read " + marker + " length=" + bodySize
                + " type=" + type, null);
        }

        return frame;
    }

}
