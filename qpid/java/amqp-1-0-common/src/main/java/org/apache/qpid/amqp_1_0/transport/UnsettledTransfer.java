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
package org.apache.qpid.amqp_1_0.transport;

import org.apache.qpid.amqp_1_0.type.transport.Transfer;

public class UnsettledTransfer
{
    private final Transfer _transfer;
    private final LinkEndpoint _linkEndpoint;

    public UnsettledTransfer(final Transfer transfer, final LinkEndpoint linkEndpoint)
    {
        _transfer = transfer;
        _linkEndpoint = linkEndpoint;
    }

    public Transfer getTransfer()
    {
        return _transfer;
    }

    public LinkEndpoint getLinkEndpoint()
    {
        return _linkEndpoint;
    }

    @Override public String toString()
    {
        return "UnsettledTransfer{" +
               "_transfer=" + _transfer +
               ", _linkEndpoint=" + _linkEndpoint +
               '}';
    }
}
