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
package org.apache.qpid.server.virtualhost;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import org.apache.qpid.common.Closeable;
import org.apache.qpid.server.binding.BindingFactory;
import org.apache.qpid.server.configuration.ConfigStore;
import org.apache.qpid.server.configuration.VirtualHostConfig;
import org.apache.qpid.server.configuration.VirtualHostConfiguration;
import org.apache.qpid.server.connection.IConnectionRegistry;
import org.apache.qpid.server.exchange.ExchangeFactory;
import org.apache.qpid.server.exchange.ExchangeRegistry;
import org.apache.qpid.server.federation.BrokerLink;
import org.apache.qpid.server.management.ManagedObject;
import org.apache.qpid.server.queue.QueueRegistry;
import org.apache.qpid.server.registry.IApplicationRegistry;
import org.apache.qpid.server.security.SecurityManager;
import org.apache.qpid.server.security.auth.manager.AuthenticationManager;
import org.apache.qpid.server.stats.StatisticsGatherer;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.txn.DtxRegistry;

public interface VirtualHost extends DurableConfigurationStore.Source, VirtualHostConfig, Closeable, StatisticsGatherer
{
    IConnectionRegistry getConnectionRegistry();

    VirtualHostConfiguration getConfiguration();

    String getName();

    QueueRegistry getQueueRegistry();

    ExchangeRegistry getExchangeRegistry();

    ExchangeFactory getExchangeFactory();

    MessageStore getMessageStore();

    DurableConfigurationStore getDurableConfigurationStore();

    AuthenticationManager getAuthenticationManager();

    SecurityManager getSecurityManager();

    void close();

    ManagedObject getManagedObject();

    UUID getBrokerId();

    void scheduleHouseKeepingTask(long period, HouseKeepingTask task);

    long getHouseKeepingTaskCount();

    public long getHouseKeepingCompletedTaskCount();

    int getHouseKeepingPoolSize();

    void setHouseKeepingPoolSize(int newSize);    

    int getHouseKeepingActiveCount();

    IApplicationRegistry getApplicationRegistry();

    BindingFactory getBindingFactory();

    void createBrokerConnection(String transport,
                                String host,
                                int port,
                                String vhost,
                                boolean durable,
                                String authMechanism, String username, String password);

    public BrokerLink createBrokerConnection(UUID id, long createTime, Map<String,String> arguments);

    ConfigStore getConfigStore();

    DtxRegistry getDtxRegistry();

    void removeBrokerConnection(BrokerLink brokerLink);

    ScheduledFuture<?> scheduleTask(long delay, Runnable timeoutTask);
}
