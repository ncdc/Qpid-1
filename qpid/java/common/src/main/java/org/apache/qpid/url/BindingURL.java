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
package org.apache.qpid.url;

import org.apache.qpid.framing.AMQShortString;

/*
    Binding URL format:
    <exch_class>://<exch_name>/[<destination>]/[<queue>]?<option>='<value>'[,<option>='<value>']*
*/
public interface BindingURL
{
    public static final String OPTION_EXCLUSIVE = "exclusive";
    public static final String OPTION_AUTODELETE = "autodelete";
    public static final String OPTION_DURABLE = "durable";
    public static final String OPTION_BROWSE = "browse";
    public static final String OPTION_CLIENTID = "clientid";
    public static final String OPTION_SUBSCRIPTION = "subscription";
    public static final String OPTION_ROUTING_KEY = "routingkey";
    public static final String OPTION_BINDING_KEY = "bindingkey";

    /**
     * This option is only applicable for 0-8/0-9/0-9-1 protocols connection
     * <p>
     * It tells the client to delegate the requeue/DLQ decision to the
     * server .If this option is not specified, the messages won't be moved to
     * the DLQ (or dropped) when delivery count exceeds the maximum.
     */
    public static final String OPTION_REJECT_BEHAVIOUR = "rejectbehaviour";


    String getURL();

    AMQShortString getExchangeClass();

    AMQShortString getExchangeName();

    AMQShortString getDestinationName();

    AMQShortString getQueueName();

    String getOption(String key);

    boolean containsOption(String key);

    AMQShortString getRoutingKey();

    AMQShortString[] getBindingKeys();

    String toString();
}
