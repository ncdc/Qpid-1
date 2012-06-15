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
package org.apache.qpid.disttest.controller.config;

import java.io.Reader;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.qpid.disttest.ConfigFileHelper;
import org.apache.qpid.disttest.client.property.PropertyValue;

public class ConfigReaderTest extends TestCase
{
    private Config _config;

    @Override
    protected void setUp()
    {
        ConfigReader configReader = new ConfigReader();
        Reader reader = ConfigFileHelper.getConfigFileReader(getClass(), "sampleConfig.json");
        _config = configReader.readConfig(reader);
    }

    public void testReadTest()
    {
        List<TestConfig> tests = _config.getTestConfigs();
        assertEquals("Unexpected number of tests", 2, tests.size());
        TestConfig test1Config = tests.get(0);
        assertNotNull("Test 1 configuration is expected", test1Config);
        assertEquals("Unexpected test name", "Test 1", test1Config.getName());

        TestConfig test2Config = tests.get(1);
        assertNotNull("Test 2 configuration is expected", test2Config);
    }

    public void testReadsTestWithQueues()
    {
        TestConfig test1Config =  _config.getTestConfigs().get(0);
        List<QueueConfig> queues = test1Config.getQueues();
        assertEquals("Unexpected number of queues", 2, queues.size());
        QueueConfig queue1Config = queues.get(0);
        assertNotNull("Expected queue 1 config", queue1Config);
        assertEquals("Unexpected queue name", "Json-Queue-Name", queue1Config.getName());
        assertTrue("Unexpected attributes", queue1Config.getAttributes().isEmpty());
        assertFalse("Unexpected durable", queue1Config.isDurable());

        QueueConfig queue2Config = queues.get(1);
        assertNotNull("Expected queue 2 config", queue2Config);
        assertEquals("Unexpected queue name", "Json Queue Name 2", queue2Config.getName());
        assertTrue("Unexpected durable", queue2Config.isDurable());
        Map<String, Object> attributes =  queue2Config.getAttributes();
        assertNotNull("Expected attributes", attributes);
        assertFalse("Attributes are not loaded", attributes.isEmpty());
        assertEquals("Unexpected number of attributes", 1, attributes.size());
        assertEquals("Unexpected attribute 'x-qpid-priorities' value", 10,
                ((Number)attributes.get("x-qpid-priorities")).intValue());
    }

    public void testReadsTestWithIterations()
    {
        TestConfig testConfig = _config.getTestConfigs().get(0);
        List<IterationValue> iterationValues = testConfig.getIterationValues();
        assertEquals("Unexpected number of iterations", 2, iterationValues.size());

        IterationValue iteration1 = iterationValues.get(0);

        String messageSizeProperty = "_messageSize";

        assertEquals("Unexpected value for property " + messageSizeProperty,
                "100",
                iteration1.getIterationPropertyValuesWithUnderscores().get(messageSizeProperty));
    }

    public void testReadsMessageProviders()
    {
        TestConfig testConfig = _config.getTestConfigs().get(0);
        ClientConfig cleintConfig = testConfig.getClients().get(0);
        List<MessageProviderConfig> configs = cleintConfig.getMessageProviders();
        assertNotNull("Message provider configs should not be null", configs);
        assertEquals("Unexpected number of message providers", 1, configs.size());
        MessageProviderConfig messageProvider = configs.get(0);
        assertNotNull("Message provider config should not be null", messageProvider);
        assertEquals("Unexpected provider name", "testProvider1", messageProvider.getName());
        Map<String, PropertyValue> properties = messageProvider.getMessageProperties();
        assertNotNull("Message properties should not be null", properties);
        assertEquals("Unexpected number of message properties", 3, properties.size());
        assertNotNull("test property is not found", properties.get("test"));
        assertNotNull("priority property is not found", properties.get("priority"));
        assertNotNull("id property is not found", properties.get("id"));
     }

}
