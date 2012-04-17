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
package org.apache.qpid.server.store.berkeleydb.tuple;

import java.util.UUID;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

import org.apache.qpid.server.store.berkeleydb.entry.QueueEntryKey;

public class QueueEntryBinding extends TupleBinding<QueueEntryKey>
{

    private static final QueueEntryBinding INSTANCE = new QueueEntryBinding();

    public static QueueEntryBinding getInstance()
    {
        return INSTANCE;
    }

    /** private constructor forces getInstance instead */
    private QueueEntryBinding() { }

    public QueueEntryKey entryToObject(TupleInput tupleInput)
    {
        UUID queueId = new UUID(tupleInput.readLong(), tupleInput.readLong());
        long messageId = tupleInput.readLong();

        return new QueueEntryKey(queueId, messageId);
    }

    public void objectToEntry(QueueEntryKey mk, TupleOutput tupleOutput)
    {
        UUID uuid = mk.getQueueId();
        tupleOutput.writeLong(uuid.getMostSignificantBits());
        tupleOutput.writeLong(uuid.getLeastSignificantBits());
        tupleOutput.writeLong(mk.getMessageId());
    }
}