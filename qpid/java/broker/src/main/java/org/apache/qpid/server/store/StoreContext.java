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

import org.apache.log4j.Logger;
import org.apache.qpid.AMQException;
import org.apache.qpid.server.queue.AMQQueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Collections;

/**
 * A context that the store can use to associate with a transactional context. For example, it could store
 * some kind of txn id.
 *
 * @author Apache Software Foundation
 */
public class StoreContext
{
    private static final Logger _logger = Logger.getLogger(StoreContext.class);

    private static final String DEFAULT_NAME = "StoreContext";
    private String _name;
    private Object _payload;
    private Map<Long, List<AMQQueue>> _dequeueMap;
    private boolean _async;
    private boolean _inTransaction;

    public StoreContext()
    {
        this(DEFAULT_NAME);
    }

    public StoreContext(String name)
    {
        this(name, false);
    }

    /**
     * @param name         The name of this Transaction
     * @param asynchrouous Is this Transaction Asynchronous
     */
    public StoreContext(String name, boolean asynchrouous)
    {
        _name = name;
        _async = asynchrouous;
        _inTransaction = false;
        _dequeueMap = Collections.synchronizedMap(new HashMap<Long, List<AMQQueue>>());
    }

    public StoreContext(boolean asynchronous)
    {
        this(DEFAULT_NAME, asynchronous);
    }

    public Object getPayload()
    {
        return _payload;
    }

    public void setPayload(Object payload)
    {
        if (_logger.isDebugEnabled())
        {
            _logger.debug("public void setPayload(Object payload = " + payload + "): called");
        }
        _payload = payload;
    }

    /**
     * Prints out the transactional context as a string, mainly for debugging purposes.
     *
     * @return The transactional context as a string.
     */
    public String toString()
    {
        return "<_name = " + _name + ", _payload = " + _payload + ">";
    }

    public Map<Long, List<AMQQueue>> getDequeueMap()
    {
        return _dequeueMap;
    }

    /**
     * Record the dequeue for processing after the commit
     *
     * @param queue
     * @param messageId
     *
     * @throws AMQException
     */
    public void dequeueMessage(AMQQueue queue, Long messageId) throws AMQException
    {
        List<AMQQueue> dequeues = _dequeueMap.get(messageId);

        synchronized (_dequeueMap)
        {
            if (dequeues == null)
            {
                dequeues = Collections.synchronizedList(new ArrayList<AMQQueue>());
                _dequeueMap.put(messageId, dequeues);
            }
        }

        dequeues.add(queue);
        if (_logger.isInfoEnabled())
        {
            _logger.info("Added (" + messageId + ") to dequeues:" + dequeues);
        }
    }

    public void beginTransaction() throws AMQException
    {
        _inTransaction = true;
    }

    public void commitTransaction() throws AMQException
    {
        _dequeueMap.clear();
        _inTransaction = false;
    }

    public void abortTransaction() throws AMQException
    {
        _dequeueMap.clear();
        _inTransaction = false;
    }

    public boolean inTransaction()
    {
        return _inTransaction;
    }

    public boolean isAsync()
    {
        return _async;
    }
}
