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

public class ContentHeaderBodyFactory implements BodyFactory
{
    private static final Logger _log = LoggerFactory.getLogger(AMQMethodBodyFactory.class);

    private static final ContentHeaderBodyFactory _instance = new ContentHeaderBodyFactory();

    public static ContentHeaderBodyFactory getInstance()
    {
        return _instance;
    }

    private ContentHeaderBodyFactory()
    {
        _log.debug("Creating content header body factory");
    }

    public AMQBody createBody(MarkableDataInput in, long bodySize) throws AMQFrameDecodingException, IOException
    {
        // all content headers are the same - it is only the properties that differ.
        // the content header body further delegates construction of properties
        return new ContentHeaderBody(in, bodySize);
    }
}
