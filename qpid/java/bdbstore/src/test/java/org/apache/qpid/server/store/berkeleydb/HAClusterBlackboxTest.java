/*
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
package org.apache.qpid.server.store.berkeleydb;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Session;

import org.apache.log4j.Logger;
import org.apache.qpid.client.AMQConnection;
import org.apache.qpid.jms.ConnectionListener;
import org.apache.qpid.jms.ConnectionURL;
import org.apache.qpid.test.utils.QpidBrokerTestCase;

/**
 * The HA black box tests test the BDB cluster as a opaque unit.  Client connects to
 * the cluster via a failover url
 *
 * @see HAClusterWhiteboxTest
 */
public class HAClusterBlackboxTest extends QpidBrokerTestCase
{
    protected static final Logger LOGGER = Logger.getLogger(HAClusterBlackboxTest.class);

    private static final String VIRTUAL_HOST = "test";
    private static final int NUMBER_OF_NODES = 3;

    private final HATestClusterCreator _clusterCreator = new HATestClusterCreator(this, VIRTUAL_HOST, NUMBER_OF_NODES);

    private FailoverAwaitingListener _failoverAwaitingListener;
    private ConnectionURL _brokerFailoverUrl;

    @Override
    protected void setUp() throws Exception
    {
        _brokerType = BrokerType.SPAWNED;

        assertTrue(isJavaBroker());
        assertTrue(isBrokerStorePersistent());

        setSystemProperty("java.util.logging.config.file", "etc" + File.separator + "log.properties");

        _clusterCreator.configureClusterNodes();

        _brokerFailoverUrl = _clusterCreator.getConnectionUrlForAllClusterNodes();

        _clusterCreator.startCluster();
        _failoverAwaitingListener = new FailoverAwaitingListener();

        super.setUp();
    }

    @Override
    public void startBroker() throws Exception
    {
        // Don't start default broker provided by QBTC.
    }

    public void testLossOfMasterNodeCausesClientToFailover() throws Exception
    {
        final Connection connection = getConnection(_brokerFailoverUrl);

        ((AMQConnection)connection).setConnectionListener(_failoverAwaitingListener);

        final int activeBrokerPort = _clusterCreator.getBrokerPortNumberFromConnection(connection);
        LOGGER.info("Active connection port " + activeBrokerPort);

        _clusterCreator.stopNode(activeBrokerPort);
        LOGGER.info("Node is stopped");
        _failoverAwaitingListener.assertFailoverOccurs(20000);
        LOGGER.info("Listener has finished");
        // any op to ensure connection remains
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public void testLossOfReplicaNodeDoesNotCauseClientToFailover() throws Exception
    {
        LOGGER.info("Connecting to " + _brokerFailoverUrl);
        final Connection connection = getConnection(_brokerFailoverUrl);
        LOGGER.info("Got connection to cluster");

        ((AMQConnection)connection).setConnectionListener(_failoverAwaitingListener);
        final int activeBrokerPort = _clusterCreator.getBrokerPortNumberFromConnection(connection);
        LOGGER.info("Active connection port " + activeBrokerPort);
        final int inactiveBrokerPort = _clusterCreator.getPortNumberOfAnInactiveBroker(connection);

        LOGGER.info("Stopping inactive broker on port " + inactiveBrokerPort);

        _clusterCreator.stopNode(inactiveBrokerPort);

        _failoverAwaitingListener.assertFailoverDoesNotOccur(2000);

        // any op to ensure connection remains
        connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    private final class FailoverAwaitingListener implements ConnectionListener
    {
        private final CountDownLatch _failoverLatch = new CountDownLatch(1);

        @Override
        public boolean preResubscribe()
        {
            return true;
        }

        @Override
        public boolean preFailover(boolean redirect)
        {
            return true;
        }

        public void assertFailoverOccurs(long delay) throws InterruptedException
        {
            _failoverLatch.await(delay, TimeUnit.MILLISECONDS);
            assertEquals("Failover did not occur", 0, _failoverLatch.getCount());
        }

        public void assertFailoverDoesNotOccur(long delay) throws InterruptedException
        {
            _failoverLatch.await(delay, TimeUnit.MILLISECONDS);
            assertEquals("Failover occurred unexpectedly", 1L, _failoverLatch.getCount());
        }


        @Override
        public void failoverComplete()
        {
            _failoverLatch.countDown();
        }

        @Override
        public void bytesSent(long count)
        {
        }

        @Override
        public void bytesReceived(long count)
        {
        }
    }

}
