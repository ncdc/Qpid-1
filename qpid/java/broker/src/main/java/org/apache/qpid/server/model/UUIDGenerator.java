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
package org.apache.qpid.server.model;

import java.util.UUID;

import org.apache.qpid.exchange.ExchangeDefaults;


public class UUIDGenerator
{

    public static UUID generateUUID()
    {
        return UUID.randomUUID();
    }

    public static UUID generateUUID(String objectName, String virtualHostName)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(virtualHostName).append(objectName);
        return UUID.nameUUIDFromBytes(sb.toString().getBytes());
    }

    public static UUID generateExchangeUUID(String exchangeName, String virtualHostName)
    {
        if(ExchangeDefaults.DEFAULT_EXCHANGE_NAME.asString().equals(exchangeName) || exchangeName.startsWith("amq.") || exchangeName.startsWith("qpid."))
        {
            return generateUUID(exchangeName, virtualHostName);
        }
        else
        {
            return generateUUID();
        }
    }
}
