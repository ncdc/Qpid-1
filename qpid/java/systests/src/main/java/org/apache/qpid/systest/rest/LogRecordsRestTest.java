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
package org.apache.qpid.systest.rest;

import java.util.List;
import java.util.Map;

public class LogRecordsRestTest extends QpidRestTestCase
{
    public void testGet() throws Exception
    {
        List<Map<String, Object>> logs = getRestTestHelper().getJsonAsList("/rest/logrecords");
        assertNotNull("Logs data cannot be null", logs);
        assertTrue("Logs are not found", logs.size() > 0);
        Map<String, Object> record = getRestTestHelper().find("message", "[Broker] BRK-1004 : Qpid Broker Ready", logs);

        assertNotNull("BRK-1004 message is not found", record);
        assertNotNull("Message id cannot be null", record.get("id"));
        assertNotNull("Message timestamp cannot be null", record.get("timestamp"));
        assertEquals("Unexpected log level", "INFO", record.get("level"));
        assertEquals("Unexpected thread", "main", record.get("thread"));
        assertEquals("Unexpected logger", "qpid.message.broker.ready", record.get("logger"));
    }
}
