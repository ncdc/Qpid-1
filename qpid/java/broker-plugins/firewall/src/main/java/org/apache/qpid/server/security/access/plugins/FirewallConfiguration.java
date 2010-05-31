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
package org.apache.qpid.server.security.access.plugins;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.qpid.server.configuration.plugins.ConfigurationPlugin;
import org.apache.qpid.server.configuration.plugins.ConfigurationPluginFactory;

public class FirewallConfiguration extends ConfigurationPlugin
{
    public static final ConfigurationPluginFactory FACTORY = new ConfigurationPluginFactory() 
    {
        public ConfigurationPlugin newInstance(String path, Configuration config) throws ConfigurationException
        {
            ConfigurationPlugin instance = new FirewallConfiguration();
            instance.setConfiguration(path, config);
            return instance;
        }

        public List<String> getParentPaths()
        {
            return Arrays.asList("security", "virtualhosts.virtualhost.security");
        }
    };

    public String[] getElementsProcessed()
    {
        return new String[] { "firewall" };
    }

    public Configuration getConfiguration()
    {
        return _configuration.subset("firewall");
    }
}
