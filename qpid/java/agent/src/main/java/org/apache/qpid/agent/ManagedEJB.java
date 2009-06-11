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
package org.apache.qpid.agent;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.qpid.agent.binding.BindingUtils;
import org.apache.qpid.agent.binding.MethodBinding;
import org.apache.qpid.agent.binding.PropertyBinding;

public class ManagedEJB extends ManagedObjectBase
{
    protected String className;
    protected String jndiLocation;

    protected Object getEJB()
    {
        try
        {
            InitialContext ctx = new InitialContext();
            return ctx.lookup(jndiLocation);
        } catch (NamingException e)
        {
            throw new AgentException("Error looking up EJB at " + jndiLocation,
                    e);
        }
    }

    @Override
    public Object get(PropertyBinding property)
    {
        return BindingUtils.get(property, this.getEJB());
    }

    @Override
    public long getId()
    {
        return System.identityHashCode(this);
    }

    @Override
    public Class getObjectClass()
    {
        try
        {
            return Class.forName(className);
        } catch (ClassNotFoundException e)
        {
            throw new AgentException(String.format(
                    "No class named %s was found", className), e);
        }
    }

    @Override
    public Object[] invoke(MethodBinding method, Object... args)
    {
        return BindingUtils.invoke(method, this.getEJB(), args);
    }

    @Override
    public void set(PropertyBinding property, Object value)
    {
        BindingUtils.set(property, value, this.getEJB());
    }

    public String getClassName()
    {
        return className;
    }

    public void setClassName(String className)
    {
        this.className = className;
    }

    public String getJndiLocation()
    {
        return jndiLocation;
    }

    public void setJndiLocation(String jndiLocation)
    {
        this.jndiLocation = jndiLocation;
    }
}
