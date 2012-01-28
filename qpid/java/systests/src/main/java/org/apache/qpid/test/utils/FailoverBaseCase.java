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
package org.apache.qpid.test.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.qpid.client.AMQConnectionFactory;
import org.apache.qpid.util.FileUtils;

import javax.naming.NamingException;

public class FailoverBaseCase extends QpidBrokerTestCase
{
    protected static final Logger _logger = LoggerFactory.getLogger(FailoverBaseCase.class);

    public static final long DEFAULT_FAILOVER_TIME = 10000L;

    protected void setUp() throws java.lang.Exception
    {
        super.setUp();
        startBroker(getFailingPort());
    }

    /**
     * We are using failover factories
     *
     * @return a connection 
     * @throws Exception
     */
    @Override
    public AMQConnectionFactory getConnectionFactory() throws NamingException
    {
        _logger.info("get ConnectionFactory");
        if (_connectionFactory == null)
        {
            if (Boolean.getBoolean("profile.use_ssl"))
            {
                _connectionFactory = getConnectionFactory("failover.ssl");
            }
            else
            {
                _connectionFactory = getConnectionFactory("failover");
            }
        }
        return _connectionFactory;
    }

    public void tearDown() throws Exception
    {
        try
        {
            super.tearDown();
        }
        finally
        {
            // Ensure we shutdown any secondary brokers, even if we are unable
            // to cleanly tearDown the QTC.
            stopBroker(getFailingPort());
            FileUtils.deleteDirectory(System.getProperty("QPID_WORK") + "/" + getFailingPort());
        }
    }

    public void failBroker(int port)
    {
        try
        {
            //TODO: use killBroker instead
            stopBroker(port);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
}
