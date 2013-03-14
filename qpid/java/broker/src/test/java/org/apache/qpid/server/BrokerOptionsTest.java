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
package org.apache.qpid.server;

import java.io.File;

import org.apache.qpid.test.utils.QpidTestCase;

public class BrokerOptionsTest extends QpidTestCase
{
    private BrokerOptions _options;

    protected void setUp()
    {
        _options = new BrokerOptions();
    }

    public void testDefaultConfigurationStoreType()
    {
        assertEquals("json", _options.getConfigurationStoreType());
    }

    public void testOverriddenConfigurationStoreType()
    {
        _options.setConfigurationStoreType("dby");
        assertEquals("dby", _options.getConfigurationStoreType());
    }

    public void testDefaultConfigurationStoreLocationWithQpidWork()
    {
        String qpidWork = "/test/value";
        setTestSystemProperty("QPID_WORK", qpidWork);

        String expectedPath = new File(qpidWork, BrokerOptions.DEFAULT_CONFIG_NAME_PREFIX + "." + BrokerOptions.DEFAULT_STORE_TYPE).getAbsolutePath();
        assertEquals (expectedPath, _options.getConfigurationStoreLocation());
    }

    public void testDefaultConfigurationStoreLocationWithoutQpidWork()
    {
        setTestSystemProperty("QPID_WORK", null);
        String userDir = System.getProperty("user.dir");

        String expectedPath = new File(userDir, "work/" + BrokerOptions.DEFAULT_CONFIG_NAME_PREFIX + "." + BrokerOptions.DEFAULT_STORE_TYPE).getAbsolutePath();
        assertEquals (expectedPath, _options.getConfigurationStoreLocation());
    }

    public void testDefaultConfigurationStoreLocationWithQpidWorkAndDifferentStoreType()
    {
        String qpidWork = "/test/value";
        setTestSystemProperty("QPID_WORK", qpidWork);

        String storeType = "dby";
        _options.setConfigurationStoreType(storeType);

        String expectedPath = new File(qpidWork, BrokerOptions.DEFAULT_CONFIG_NAME_PREFIX + "." + storeType).getAbsolutePath();
        assertEquals (expectedPath, _options.getConfigurationStoreLocation());
    }

    public void testOverriddenConfigurationStoreLocation()
    {
        final String testConfigFile = "/my/test/store-location.dby";
        _options.setConfigurationStoreLocation(testConfigFile);
        assertEquals(testConfigFile, _options.getConfigurationStoreLocation());
    }

    public void testDefaultLogConfigFile()
    {
        assertNull(_options.getLogConfigFile());
    }

    public void testOverriddenLogConfigFile()
    {
        final String testLogConfigFile = "etc/mytestlog4j.xml";
        _options.setLogConfigFile(testLogConfigFile);
        assertEquals(testLogConfigFile, _options.getLogConfigFile());
    }

    public void testDefaultLogWatchFrequency()
    {
        assertEquals(0L, _options.getLogWatchFrequency());
    }

    public void testOverridenLogWatchFrequency()
    {
        final int myFreq = 10 * 1000;
        
        _options.setLogWatchFrequency(myFreq);
        assertEquals(myFreq, _options.getLogWatchFrequency());
    }

    public void testDefaultInitialConfigurationLocation()
    {
        assertEquals(BrokerOptions.DEFAULT_INITIAL_CONFIG_LOCATION, _options.getInitialConfigurationLocation());
    }

    public void testOverriddenInitialConfigurationLocation()
    {
        final String testConfigFile = "etc/mytestconfig.json";
        _options.setInitialConfigurationLocation(testConfigFile);
        assertEquals(testConfigFile, _options.getInitialConfigurationLocation());
    }

    public void testDefaultManagementMode()
    {
        assertEquals(false, _options.isManagementMode());
    }

    public void testOverriddenDefaultManagementMode()
    {
        _options.setManagementMode(true);
        assertEquals(true, _options.isManagementMode());
    }

    public void testDefaultManagementModeRmiPort()
    {
        assertEquals(0, _options.getManagementModeRmiPort());
    }

    public void testOverriddenDefaultManagementModeRmiPort()
    {
        _options.setManagementModeRmiPort(5555);
        assertEquals(5555, _options.getManagementModeRmiPort());
    }

    public void testDefaultManagementModeConnectorPort()
    {
        assertEquals(0, _options.getManagementModeConnectorPort());
    }

    public void testOverriddenDefaultManagementModeConnectorPort()
    {
        _options.setManagementModeConnectorPort(5555);
        assertEquals(5555, _options.getManagementModeConnectorPort());
    }

    public void testDefaultManagementModeHttpPort()
    {
        assertEquals(0, _options.getManagementModeHttpPort());
    }

    public void testOverriddenDefaultManagementModeHttpPort()
    {
        _options.setManagementModeHttpPort(5555);
        assertEquals(5555, _options.getManagementModeHttpPort());
    }

    public void testDefaultWorkDirWithQpidWork()
    {
        String qpidWork = "/test/value";
        setTestSystemProperty("QPID_WORK", qpidWork);

        String expectedPath = new File(qpidWork).getAbsolutePath();
        assertEquals (expectedPath, _options.getWorkDir());
    }

    public void testDefaultWorkDirWithoutQpidWork()
    {
        setTestSystemProperty("QPID_WORK", null);
        String userDir = System.getProperty("user.dir");

        String expectedPath = new File(userDir, "work").getAbsolutePath();
        assertEquals (expectedPath, _options.getWorkDir());
    }

    public void testOverriddenWorkDir()
    {
        final String testWorkDir = "/my/test/work/dir";
        _options.setWorkDir(testWorkDir);
        assertEquals(testWorkDir, _options.getWorkDir());
    }
}
