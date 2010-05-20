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
package org.apache.qpid.server.configuration.plugin;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConversionException;
import org.apache.qpid.server.configuration.plugins.ConfigurationPlugin;
import org.apache.qpid.server.configuration.plugins.ConfigurationPluginFactory;

import java.util.concurrent.TimeUnit;

public class SlowConsumerDetectionConfiguration extends ConfigurationPlugin
{
    public static class SlowConsumerDetectionConfigurationFactory implements ConfigurationPluginFactory
    {
        public ConfigurationPlugin newInstance(String path, Configuration config) throws ConfigurationException
        {
            SlowConsumerDetectionConfiguration slowConsumerConfig = new SlowConsumerDetectionConfiguration();
            slowConsumerConfig.setConfiguration(path, config);
            return slowConsumerConfig;
        }

        public String[] getParentPaths()
        {
            return new String[]{"virtualhosts.virtualhost.slow-consumer-detection"};
        }
    }

    //Set Default time unit to seconds
    TimeUnit _timeUnit = TimeUnit.SECONDS;

    public String[] getElementsProcessed()
    {
        return new String[]{"delay",
                            "timeunit"};
    }

    public long getDelay()
    {
        return _configuration.getLong("delay", 10);
    }

    public TimeUnit getTimeUnit()
    {
        return  _timeUnit;
    }

    @Override
    public void setConfiguration(String path, Configuration configuration) throws ConfigurationException
    {
        super.setConfiguration(path, configuration);

        //Validate Configuration

        try
        {
            long delay = _configuration.getLong("delay");
            if (delay <= 0)
            {
                throw new ConfigurationException("Slow Consumer Detection Delay must be a Positive Long value.");
            }
        }
        catch (Exception e)
        {
            Throwable last = e;

            // Find the first cause
            if (e instanceof ConversionException)
            {
                Throwable t = e.getCause();
                while (t != null)
                {
                    last = t;
                    t = last.getCause();
                }
            }

            throw new ConfigurationException("Unable to configure Slow Consumer Detection invalid delay:"+ _configuration.getString("delay"), last);
        }

        String timeUnit = _configuration.getString("timeunit");


        if (timeUnit != null)
        {
            try
            {
                _timeUnit = TimeUnit.valueOf(timeUnit.toUpperCase());
            }
            catch (IllegalArgumentException iae)
            {
                throw new ConfigurationException("Unable to configure Slow Consumer Detection invalid TimeUnit:" + timeUnit);            
            }
        }


        System.out.println("Configured SCDC");
        System.out.println("Delay:" + getDelay());
        System.out.println("TimeUnit:" + getTimeUnit());
    }
}
